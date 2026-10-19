import { act, renderHook } from '@testing-library/react';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

import { agentService } from '@/services/agent';
import { aiAgentService } from '@/services/aiAgent';
import { getSessionStoreState } from '@/store/session';
import { useUserStore } from '@/store/user';

import { useChatStore } from '../../../../store';
import { messageMapKey } from '../../../../utils/messageMapKey';
import { createMockAgentConfig, TEST_IDS } from './fixtures';
import { resetTestEnvironment, setupMockSelectors, spyOnMessageService } from './helpers';

vi.mock('../transports/hetero/heterogeneousAgentExecutor', () => ({
  executeHeterogeneousAgent: vi.fn(),
}));

vi.mock('@/libs/trpc/client', () => ({
  lambdaClient: {
    session: { updateSession: { mutate: vi.fn().mockResolvedValue(undefined) } },
  },
}));

const context = { agentId: TEST_IDS.SESSION_ID, threadId: null, topicId: TEST_IDS.TOPIC_ID };
const contextKey = messageMapKey(context);

beforeEach(() => {
  resetTestEnvironment();
  setupMockSelectors();
  spyOnMessageService();
  vi.spyOn(getSessionStoreState(), 'triggerSessionUpdate').mockResolvedValue(undefined);
  vi.spyOn(agentService, 'getAgentConfigById').mockResolvedValue(createMockAgentConfig() as any);
  vi.spyOn(aiAgentService, 'setQueuedMessages').mockResolvedValue(undefined as never);
  vi.spyOn(aiAgentService, 'listQueuedFollowUps').mockResolvedValue([]);
  useUserStore.setState({ workspaceUserPreference: {} });

  act(() => {
    useChatStore.setState({
      executeClientAgent: vi.fn(),
      executeGatewayAgent: vi.fn(),
      isGatewayModeEnabled: () => true,
      mainInputEditor: null,
      operations: {
        'op-running': {
          childOperationIds: [],
          context,
          id: 'op-running',
          metadata: { serverOperationId: 'op-server' },
          status: 'running',
          type: 'execServerAgentRuntime',
        },
      } as any,
      operationsByContext: { [contextKey]: ['op-running'] },
      refreshMessages: vi.fn(),
      refreshTopic: vi.fn(),
    });
  });
});

afterEach(() => {
  vi.restoreAllMocks();
});

describe('sendMessage behind a running Gateway run', () => {
  it('queues the follow-up on the server, so the run reads it before its next model call', async () => {
    const { result } = renderHook(() => useChatStore());
    const append = vi
      .spyOn(aiAgentService, 'appendQueuedFollowUp')
      .mockResolvedValue({ id: 'x', length: 1, queued: true });

    await act(async () => {
      await result.current.sendMessage({ context, message: 'also check the tests' });
    });

    await vi.waitFor(() =>
      expect(useChatStore.getState().queuedMessages[contextKey]?.[0]?.serverDelivery).toBe(
        'server',
      ),
    );
    const [{ id, request }] = append.mock.calls[0];
    expect(id).toBe(useChatStore.getState().queuedMessages[contextKey][0].id);
    expect(request).toMatchObject({
      agentId: TEST_IDS.SESSION_ID,
      appContext: { topicId: TEST_IDS.TOPIC_ID },
      prompt: 'also check the tests',
    });
    // The server joins it; the run is not told to hand its turn back.
    expect(aiAgentService.setQueuedMessages).not.toHaveBeenCalled();
    expect(result.current.executeGatewayAgent).not.toHaveBeenCalled();
  });

  it('keeps the follow-up in this client when the agent does not run on the Gateway', async () => {
    act(() => {
      useChatStore.setState({ isGatewayModeEnabled: () => false });
    });
    const { result } = renderHook(() => useChatStore());
    const append = vi.spyOn(aiAgentService, 'appendQueuedFollowUp');

    await act(async () => {
      await result.current.sendMessage({ context, message: 'also check the tests' });
    });

    expect(append).not.toHaveBeenCalled();
    const [queued] = useChatStore.getState().queuedMessages[contextKey];
    expect(queued.content).toBe('also check the tests');
    expect(queued.serverDelivery).toBeUndefined();
    expect(aiAgentService.setQueuedMessages).toHaveBeenCalledWith({
      operationId: 'op-server',
      pending: true,
    });
  });
});
