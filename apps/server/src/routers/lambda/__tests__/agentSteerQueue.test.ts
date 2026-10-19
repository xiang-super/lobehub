// @vitest-environment node
import { beforeEach, describe, expect, it, vi } from 'vitest';

import { SteerQueueWriteUnknownError } from '@/server/services/redis/steerQueue';

import { agentSteerQueueRouter } from '../agentSteerQueue';

const mocks = vi.hoisted(() => ({
  append: vi.fn(),
  entryState: vi.fn(),
  isRunningOperationAlive: vi.fn(),
  list: vi.fn(),
  liveCandidates: vi.fn(),
  remove: vi.fn(),
  snapshot: vi.fn(),
}));

vi.mock('@/database/models/topic', () => ({
  TopicModel: vi.fn().mockImplementation(function () {
    return { isRunningOperationAlive: mocks.isRunningOperationAlive };
  }),
}));
vi.mock('@/server/services/redis', () => ({
  redisService: {
    steerQueue: {
      append: mocks.append,
      entryState: mocks.entryState,
      remove: mocks.remove,
      snapshot: mocks.snapshot,
    },
  },
}));
vi.mock('@/server/routers/lambda/_helpers/workspaceAgentGuard', () => ({
  assertCanUseWorkspaceAgent: vi.fn(),
}));
vi.mock('@/server/routers/lambda/aiAgent/guards', () => ({
  assertCanUseAgentRunConversation: vi.fn(),
}));
vi.mock('@/business/server/trpc-middlewares/rbacPermission', async () => {
  const mod = await vi.importActual<{ trpc: any }>('@/libs/trpc/lambda/init');
  return { withScopedPermission: () => mod.trpc.middleware(async (opts: any) => opts.next()) };
});
vi.mock('@/business/server/trpc-middlewares/workspaceAuth', async () => {
  const mod = await vi.importActual<{ trpc: any }>('@/libs/trpc/lambda/init');
  return { wsCompatProcedure: mod.trpc.procedure };
});
vi.mock('@/libs/trpc/lambda/middleware', () => ({
  serverDatabase: async (opts: any) => opts.next({ ctx: opts.ctx }),
}));

// The scope / owner / status filtering itself runs against a real DB in
// agentSteerQueue.liveness.test.ts; here the query only yields candidates.
const serverDB = {
  select: () => ({
    from: () => ({
      where: () => ({ orderBy: () => ({ limit: async () => mocks.liveCandidates() }) }),
    }),
  }),
};

const caller = () =>
  agentSteerQueueRouter.createCaller({
    clientIp: '1.2.3.4',
    serverDB,
    userAgent: 'test',
    userId: 'user-1',
  } as any);

const appendInput = {
  id: 'q-1',
  request: {
    agentId: 'agt-1',
    appContext: { projectWorkingDirectoryId: 'pwd-1', topicId: 'tpc-1' },
    prompt: 'also check the tests',
    streamFeatures: ['file_works'],
  },
};

describe('agentSteerQueueRouter.append', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mocks.liveCandidates.mockReturnValue([{ id: 'op-1' }]);
    mocks.isRunningOperationAlive.mockResolvedValue(true);
    mocks.append.mockResolvedValue(1);
    mocks.remove.mockResolvedValue(true);
    mocks.entryState.mockResolvedValue('absent');
  });

  // ROOT CAUSE:
  //
  // A retry of an append whose answer was lost went through the liveness
  // check first. Had the run ended meanwhile, it answered `idle` and the
  // client sent the follow-up itself while the server still held (or had
  // delivered) the first copy — the follow-up ran twice.
  //
  // Before: liveness decided first.
  // After: what the server already holds for that id decides.
  it.each([
    ['still waiting', 'queued', 1],
    ['being delivered', 'delivering', 0],
    ['delivered', 'taken', 0],
  ] as const)(
    'answers a retry of an entry %s as queued, whatever the run',
    async (_l, state, length) => {
      mocks.entryState.mockResolvedValue(state);
      mocks.liveCandidates.mockReturnValue([]);

      await expect(caller().append(appendInput)).resolves.toEqual({
        id: 'q-1',
        length,
        queued: true,
      });
      expect(mocks.append).not.toHaveBeenCalled();
    },
  );

  it('queues behind the caller’s own live run, keeping only what a later turn can use', async () => {
    await expect(caller().append(appendInput)).resolves.toEqual({
      id: 'q-1',
      length: 1,
      queued: true,
    });

    const [scope, entry, behindOperationId] = mocks.append.mock.calls[0];
    expect(behindOperationId).toBe('op-1');
    expect(scope).toEqual({ threadId: null, topicId: 'tpc-1', userId: 'user-1' });
    expect(entry).toMatchObject({
      audit: { clientIp: '1.2.3.4', userAgent: 'test' },
      id: 'q-1',
      request: {
        acceptsFileWorks: true,
        acceptsMemberRuntimeEnd: false,
        prompt: 'also check the tests',
      },
      userId: 'user-1',
    });
    expect(entry.request.appContext.projectWorkingDirectoryId).toBeUndefined();
    expect(entry.request.streamFeatures).toBeUndefined();
  });

  it('keeps the composer state it was sent from, up to a cap', async () => {
    const editorData = { root: { children: [], type: 'root' } };
    await caller().append({ ...appendInput, composer: { editorData } });
    expect(mocks.append.mock.calls[0][1].composer).toEqual({ editorData });

    const huge = { root: { text: 'x'.repeat(40 * 1024) } };
    await caller().append({ ...appendInput, composer: { editorData: huge } });
    expect(mocks.append.mock.calls[1][1].composer).toBeUndefined();
  });

  it('tells the client to send directly when no run is live', async () => {
    mocks.isRunningOperationAlive.mockResolvedValue(false);

    await expect(caller().append(appendInput)).resolves.toEqual({ queued: false, reason: 'idle' });
    expect(mocks.append).not.toHaveBeenCalled();
  });

  it('does not queue when no own top-level run is on the surface', async () => {
    mocks.liveCandidates.mockReturnValue([]);

    await expect(caller().append(appendInput)).resolves.toEqual({ queued: false, reason: 'idle' });
    expect(mocks.append).not.toHaveBeenCalled();
  });

  it.each(['a\tb', 'a\nb', ''])(
    'rejects an entry id the queue header cannot carry: %j',
    async (id) => {
      await expect(caller().append({ ...appendInput, id })).rejects.toMatchObject({
        code: 'BAD_REQUEST',
      });
      expect(mocks.append).not.toHaveBeenCalled();
    },
  );

  it('reports queued for a retry its run already took, so the client does not send it again', async () => {
    mocks.append.mockResolvedValue('taken');

    await expect(caller().append(appendInput)).resolves.toEqual({
      id: 'q-1',
      length: 0,
      queued: true,
    });
  });

  it('refuses to guess between two live runs on the same surface', async () => {
    mocks.liveCandidates.mockReturnValue([{ id: 'op-1' }, { id: 'op-2' }]);

    await expect(caller().append(appendInput)).resolves.toEqual({
      queued: false,
      reason: 'ambiguous',
    });
    expect(mocks.append).not.toHaveBeenCalled();
  });

  it('answers neither queued nor send-directly when the write outcome is unknown', async () => {
    mocks.append.mockRejectedValue(new SteerQueueWriteUnknownError(new Error('timeout')));

    await expect(caller().append(appendInput)).rejects.toMatchObject({
      code: 'SERVICE_UNAVAILABLE',
    });
  });

  it('takes the entry back when the run ended after its drain', async () => {
    mocks.isRunningOperationAlive.mockResolvedValueOnce(true).mockResolvedValueOnce(false);

    await expect(caller().append(appendInput)).resolves.toEqual({ queued: false, reason: 'idle' });
    expect(mocks.remove).toHaveBeenCalledWith(
      { threadId: null, topicId: 'tpc-1', userId: 'user-1' },
      'q-1',
    );
  });

  it('reports queued when the drain already took the entry it tried to take back', async () => {
    mocks.isRunningOperationAlive.mockResolvedValueOnce(true).mockResolvedValueOnce(false);
    mocks.remove.mockResolvedValue(false);

    await expect(caller().append(appendInput)).resolves.toMatchObject({ queued: true });
  });

  it.each([
    [undefined, 'unavailable'],
    ['full', 'full'],
  ] as const)('falls back to a direct send when append returns %s', async (result, reason) => {
    mocks.append.mockResolvedValue(result);

    await expect(caller().append(appendInput)).resolves.toEqual({ queued: false, reason });
  });
});

describe('agentSteerQueueRouter.list / remove', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mocks.snapshot.mockResolvedValue({
      delivering: [],
      waiting: [
        { createdAt: 1, id: 'mine', request: { prompt: 'a' }, userId: 'user-1' },
        { createdAt: 2, id: 'theirs', request: { prompt: 'b' }, userId: 'user-2' },
      ],
    });
  });

  it('lists only the caller’s own follow-ups', async () => {
    await expect(caller().list({ topicId: 'tpc-1' })).resolves.toEqual([
      expect.objectContaining({ createdAt: 1, delivering: false, id: 'mine', prompt: 'a' }),
    ]);
  });

  // ROOT CAUSE:
  //
  // A drained entry left the waiting list before its run confirmed it, so a
  // client treated it as delivered and dropped it, though a failed delivery
  // returns it to the queue — invisible until another run or a reload.
  //
  // Before: only waiting entries were listed.
  // After: taken-but-unconfirmed ones are listed as `delivering`, from one
  // atomic read, with what a reloaded client needs to resend them as typed.
  it('lists follow-ups a run took but has not confirmed as delivering', async () => {
    const composer = { editorData: { root: { type: 'root' } }, filesPreview: [{ id: 'f1' }] };
    mocks.snapshot.mockResolvedValue({
      delivering: [
        {
          composer,
          createdAt: 0,
          id: 'taken',
          request: { prompt: 'z', selectedSkillIds: ['s1'] },
          userId: 'user-1',
        },
        { createdAt: 3, id: 'other', request: { prompt: 'y' }, userId: 'user-2' },
      ],
      waiting: [{ createdAt: 1, id: 'mine', request: { prompt: 'a' }, userId: 'user-1' }],
    });

    await expect(caller().list({ topicId: 'tpc-1' })).resolves.toEqual([
      expect.objectContaining({
        delivering: true,
        editorData: composer.editorData,
        filesPreview: composer.filesPreview,
        id: 'taken',
        selectedSkillIds: ['s1'],
      }),
      expect.objectContaining({ delivering: false, id: 'mine' }),
    ]);
  });

  it.each([
    ['removed', true],
    ['delivering', false],
    ['taken', false],
    ['absent', false],
  ] as const)('withdraws in one step and says where it is: %s', async (state, removed) => {
    mocks.entryState.mockResolvedValue(state);

    await expect(caller().remove({ id: 'mine', topicId: 'tpc-1' })).resolves.toEqual({
      removed,
      state,
    });
    expect(mocks.entryState).toHaveBeenCalledWith(
      { threadId: null, topicId: 'tpc-1', userId: 'user-1' },
      'mine',
      { remove: true },
    );
  });
});
