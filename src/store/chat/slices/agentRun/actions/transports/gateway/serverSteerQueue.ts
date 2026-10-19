import type { ConversationContext } from '@lobechat/types';
import debug from 'debug';

import { aiAgentService, type QueuedFollowUpRequest } from '@/services/aiAgent';
import { topicService } from '@/services/topic';
import { topicSelectors } from '@/store/chat/selectors';
import { operationSelectors } from '@/store/chat/slices/operation/selectors';
import { isClientHeldQueuedMessage, type QueuedMessage } from '@/store/chat/slices/operation/types';
import type { ChatStore } from '@/store/chat/store';
import { messageMapKey } from '@/store/chat/utils/messageMapKey';
import { isTrpcErrorCode } from '@/utils/trpcError';

import { scheduleQueuedFollowUp } from '../../lifecycle/queuedFollowUp';

const log = debug('lobe-store:server-steer-queue');

/** How often a conversation with follow-ups on the server checks which are still waiting. */
export const SERVER_QUEUE_POLL_MS = 1500;
/** Reads of the topic after the server took the follow-ups, until its next run shows up. */
const SUCCESSOR_SYNC_ATTEMPTS = 5;
/** How long to wait for the run that took the follow-ups to end here. */
const LOCAL_RUN_SETTLE_TIMEOUT_MS = 30 * 60 * 1000;
/**
 * Checks with no run here and the follow-ups still waiting on the server. The
 * run that ended took nothing (it failed or was stopped), so they come back.
 */
const ORPHANED_QUEUE_TICKS = 20;
/** An append whose outcome is unknown is retried with the same id. */
const APPEND_ATTEMPTS = 3;

type Get = () => ChatStore;

/** In-flight hand-offs, chained per conversation so the server keeps the queue order. */
const handOffChains = new Map<string, Promise<void>>();
/** In-flight hand-off of each message, so a withdrawal waits for its outcome. */
const handOffs = new Map<string, Promise<void>>();
/** In-flight reclaims per conversation: two removes of one id would drop it. */
const reclaims = new Map<string, Promise<void>>();
/** Conversations whose follow-ups on the server are being watched. */
const watchers = new Set<string>();

const queueOf = (get: Get, contextKey: string): QueuedMessage[] =>
  get().queuedMessages?.[contextKey] ?? [];

const findQueued = (get: Get, contextKey: string, id: string) =>
  queueOf(get, contextKey).find((message) => message.id === id);

const surfaceOf = (request: QueuedFollowUpRequest) => ({
  threadId: request.appContext?.threadId ?? null,
  topicId: request.appContext!.topicId!,
});

const hasLocalRun = (get: Get, context: ConversationContext) =>
  operationSelectors.getRunningQueueBlockingOperationIds(context)(get()).length > 0;

/**
 * Whether a send queued behind this conversation's run can be queued on the
 * server: a Gateway run on an existing main-topic conversation of the user's
 * own, with nothing queued ahead that only this client holds (it would be
 * overtaken).
 */
export const canQueueOnServer = (
  get: Get,
  contextKey: string,
  params: { context: ConversationContext; isGatewayRun: boolean },
): boolean => {
  const { context, isGatewayRun } = params;
  if (!isGatewayRun) return false;
  if (!context.topicId || context.threadId || context.agentShareId || context.topicShareId)
    return false;

  return !queueOf(get, contextKey).some(isClientHeldQueuedMessage);
};

/** The follow-ups only this client holds are sent by it once nothing runs. */
const sendIfIdle = (get: Get, contextKey: string, context: ConversationContext) => {
  const queue = queueOf(get, contextKey);
  if (queue.length === 0 || !queue.every(isClientHeldQueuedMessage)) return;
  if (hasLocalRun(get, context)) return;

  const operationIds = get().operationsByContext[contextKey] ?? [];
  scheduleQueuedFollowUp(get, context, operationIds.at(-1) ?? '');
};

const appendWithRetry = async (id: string, request: QueuedFollowUpRequest) => {
  let lastError: unknown;
  for (let attempt = 0; attempt < APPEND_ATTEMPTS; attempt++) {
    try {
      return await aiAgentService.appendQueuedFollowUp({ id, request });
    } catch (error) {
      lastError = error;
      if (!isTrpcErrorCode(error, 'SERVICE_UNAVAILABLE')) break;
    }
  }
  throw lastError;
};

/**
 * Hand a queued message to the server queue behind the running Gateway run.
 * Kept by the server, it joins that run before its next model call or starts
 * as the next turn, also with this tab closed. Anything else keeps it here,
 * where it is sent once the run ends, as before.
 */
export const handOffQueuedMessage = (
  get: Get,
  contextKey: string,
  context: ConversationContext,
  id: string,
  buildRequest?: () => Promise<QueuedFollowUpRequest>,
): Promise<void> => {
  const run = async () => {
    const keepHere = () => {
      get().updateQueuedMessage(contextKey, id, { serverDelivery: undefined });
      get().internal_syncQueuedMessagesFlag(contextKey);
      sendIfIdle(get, contextKey, context);
    };

    const queue = queueOf(get, contextKey);
    const index = queue.findIndex((message) => message.id === id);
    if (index < 0) return;
    // Something ahead of it stayed here: the server would deliver this one first.
    if (queue.slice(0, index).some(isClientHeldQueuedMessage)) return keepHere();

    let request = queue[index].serverRequest;
    if (!request && buildRequest) {
      try {
        request = await buildRequest();
        get().updateQueuedMessage(contextKey, id, { serverRequest: request });
      } catch (error) {
        console.error('[serverSteerQueue] building the queued request failed:', error);
      }
    }
    if (!request) return keepHere();

    get().updateQueuedMessage(contextKey, id, { serverDelivery: 'appending' });
    let queued = false;
    try {
      queued = (await appendWithRetry(id, request)).queued;
    } catch (error) {
      // Unknown whether it was stored. Keeping it here can at worst send it
      // twice; dropping it could lose it.
      console.error('[serverSteerQueue] queueing on the server failed:', error);
    }

    if (!findQueued(get, contextKey, id)) {
      // Withdrawn while the append was in flight.
      if (queued)
        await aiAgentService.removeQueuedFollowUp({ ...surfaceOf(request), id }).catch(() => {});
      return;
    }
    if (!queued) return keepHere();

    log('[%s] %s queued on the server', contextKey, id);
    get().updateQueuedMessage(contextKey, id, { serverDelivery: 'server' });
    watchServerQueue(get, contextKey, context);
  };

  const previous = handOffChains.get(contextKey) ?? Promise.resolve();
  const handOff = previous.then(run, run);
  const settled = handOff.finally(() => {
    handOffs.delete(id);
    if (handOffChains.get(contextKey) === settled) handOffChains.delete(contextKey);
  });
  handOffChains.set(contextKey, settled);
  handOffs.set(id, settled);

  return settled;
};

/**
 * After the server took its follow-ups with no run of ours left locally, it
 * started them as the next run: publish that run's marker so the conversation
 * reconnects to it.
 */
const syncQueuedSuccessorRun = async (get: Get, context: ConversationContext) => {
  const { topicId } = context;
  if (!topicId) return;
  const knownOperationId =
    topicSelectors.getTopicById(topicId)(get())?.metadata?.runningOperation?.operationId;

  for (let attempt = 0; attempt < SUCCESSOR_SYNC_ATTEMPTS; attempt++) {
    if (hasLocalRun(get, context)) return;

    const topic = await topicService.getTopicDetail(topicId).catch(() => undefined);
    const runningOperation = topic?.metadata?.runningOperation;
    if (runningOperation && runningOperation.operationId !== knownOperationId) {
      const current = topicSelectors.getTopicById(topicId)(get());
      if (!current) return;
      get().internal_dispatchTopic({
        id: topicId,
        type: 'updateTopic',
        value: { metadata: { ...current.metadata, runningOperation } },
      });
      if (get().activeTopicId === topicId) await get().refreshMessages();
      return;
    }

    await new Promise((resolve) => setTimeout(resolve, SERVER_QUEUE_POLL_MS));
  }
};

const onServerQueueDelivered = async (
  get: Get,
  contextKey: string,
  context: ConversationContext,
) => {
  // Joined into the run, or taken by it as it ended — its end may not have
  // reached this client yet. Either way, the next run (if any) is known once
  // this one settles.
  const settleBy = Date.now() + LOCAL_RUN_SETTLE_TIMEOUT_MS;
  while (hasLocalRun(get, context) && Date.now() < settleBy)
    await new Promise((resolve) => setTimeout(resolve, SERVER_QUEUE_POLL_MS));
  if (!hasLocalRun(get, context)) await syncQueuedSuccessorRun(get, context);

  // Queued here behind the delivered ones: the server can take them now.
  const left = queueOf(get, contextKey).filter(
    (message) => isClientHeldQueuedMessage(message) && message.serverRequest,
  );
  if (left.length === 0) return sendIfIdle(get, contextKey, context);
  for (const message of left) void handOffQueuedMessage(get, contextKey, context, message.id);
};

/**
 * Follow the conversation's follow-ups on the server until all of them left
 * its queue — joined into the run or started as the next turn — and drop
 * each from the tray as it goes.
 */
export const watchServerQueue = (get: Get, contextKey: string, context: ConversationContext) => {
  if (watchers.has(contextKey)) return;
  watchers.add(contextKey);
  let ticksWithoutRun = 0;

  const tick = async () => {
    const held = queueOf(get, contextKey).filter(
      (message) => message.serverDelivery === 'server' && message.serverRequest,
    );
    if (held.length === 0) {
      watchers.delete(contextKey);
      return;
    }

    try {
      const waiting = new Set(
        (await aiAgentService.listQueuedFollowUps(surfaceOf(held[0].serverRequest!))).map(
          (entry) => entry.id,
        ),
      );
      const delivered = held.filter((message) => !waiting.has(message.id));
      for (const message of delivered) {
        if (findQueued(get, contextKey, message.id)?.serverDelivery === 'server')
          get().removeQueuedMessage(contextKey, message.id);
      }
      const stillHeld = queueOf(get, contextKey).some(
        (message) => message.serverDelivery === 'server',
      );
      if (delivered.length > 0 && !stillHeld) {
        watchers.delete(contextKey);
        await onServerQueueDelivered(get, contextKey, context);
        return;
      }
    } catch (error) {
      console.error('[serverSteerQueue] reading the server queue failed:', error);
    }

    ticksWithoutRun = hasLocalRun(get, context) ? 0 : ticksWithoutRun + 1;
    // Stopped elsewhere: that run starts nothing next, take them back now.
    const stopped =
      operationSelectors.getIdleQueueHost(context)(get())?.metadata.endedWithoutCompletion;
    if (stopped || ticksWithoutRun >= ORPHANED_QUEUE_TICKS) {
      watchers.delete(contextKey);
      await reclaimServerQueuedMessages(get, contextKey);
      return;
    }

    setTimeout(tick, SERVER_QUEUE_POLL_MS);
  };

  setTimeout(tick, SERVER_QUEUE_POLL_MS);
};

/**
 * Put the follow-ups still waiting on the server back in the tray of a
 * conversation this page reconnects to (after a reload, or opened elsewhere),
 * and follow them again, so they show as queued and the run that takes them is
 * picked up here.
 */
export const restoreServerQueuedMessages = async (
  get: Get,
  context: ConversationContext,
): Promise<void> => {
  const { agentId, topicId } = context;
  if (!agentId || !topicId) return;
  if (context.threadId || context.agentShareId || context.topicShareId) return;

  const surface = { threadId: null, topicId };
  const waiting = await aiAgentService.listQueuedFollowUps(surface).catch((error) => {
    console.error('[serverSteerQueue] reading the server queue failed:', error);
    return [];
  });
  if (waiting.length === 0) return;

  const contextKey = messageMapKey(context);
  const known = new Set(queueOf(get, contextKey).map((message) => message.id));
  for (const entry of waiting) {
    if (known.has(entry.id)) continue;
    get().enqueueMessage(contextKey, {
      content: entry.prompt,
      createdAt: entry.createdAt,
      ...(entry.fileIds?.length && { files: entry.fileIds }),
      id: entry.id,
      interruptMode: 'soft',
      serverDelivery: 'server',
      serverRequest: {
        agentId,
        appContext: { threadId: null, topicId },
        fileIds: entry.fileIds ?? undefined,
        prompt: entry.prompt,
      },
    });
  }
  log('[%s] restored %d follow-ups from the server', contextKey, waiting.length);
  watchServerQueue(get, contextKey, context);
};

/**
 * Take the follow-ups back from the server queue, so only this client holds
 * them again — for a stopped run, which starts nothing next. The ones the
 * server already took are gone from the tray.
 */
export const reclaimServerQueuedMessages = (get: Get, contextKey: string): Promise<void> => {
  const inFlight = reclaims.get(contextKey);
  if (inFlight) return inFlight;

  const reclaim = (async () => {
    await handOffChains.get(contextKey);

    for (const message of queueOf(get, contextKey)) {
      if (message.serverDelivery !== 'server' || !message.serverRequest) continue;
      try {
        const { removed } = await aiAgentService.removeQueuedFollowUp({
          ...surfaceOf(message.serverRequest),
          id: message.id,
        });
        if (removed)
          get().updateQueuedMessage(contextKey, message.id, { serverDelivery: undefined });
        else get().removeQueuedMessage(contextKey, message.id);
      } catch (error) {
        // Still on the server, which delivers it on the conversation's next run.
        console.error('[serverSteerQueue] taking a follow-up back failed:', error);
      }
    }
  })().finally(() => reclaims.delete(contextKey));
  reclaims.set(contextKey, reclaim);

  return reclaim;
};

/**
 * Withdraw one queued message from the tray, and from the server when it is
 * there. `false` means the server already took it to deliver, so it must not
 * be sent or edited again.
 */
export const withdrawQueuedMessage = async (
  get: Get,
  contextKey: string,
  id: string,
): Promise<boolean> => {
  await handOffs.get(id);
  const message = findQueued(get, contextKey, id);
  if (!message) return false;

  if (message.serverDelivery === 'server' && message.serverRequest) {
    const { removed } = await aiAgentService.removeQueuedFollowUp({
      ...surfaceOf(message.serverRequest),
      id,
    });
    get().removeQueuedMessage(contextKey, id);
    return removed;
  }

  get().removeQueuedMessage(contextKey, id);
  return true;
};
