import type { ConversationContext, UploadFileItem } from '@lobechat/types';

import { operationSelectors } from '@/store/chat/slices/operation/selectors';
import {
  mergeQueuedMessages,
  type QueuedMessage,
  reconstructUploadFilesFromQueue,
} from '@/store/chat/slices/operation/types';
import type { ChatStore } from '@/store/chat/store';
import { messageMapKey } from '@/store/chat/utils/messageMapKey';

/** Send drained queued messages as one steered turn. */
export const sendQueuedMessages = (
  get: () => ChatStore,
  context: ConversationContext,
  messages: QueuedMessage[],
): Promise<unknown> => {
  const merged = mergeQueuedMessages(messages);
  const mergedFiles =
    merged.filesPreview.length > 0
      ? reconstructUploadFilesFromQueue(merged.filesPreview)
      : merged.files.length > 0
        ? (merged.files.map((id) => ({ id })) as UploadFileItem[])
        : undefined;

  return get().sendMessage({
    context,
    editorData: merged.editorData,
    files: mergedFiles,
    ...(merged.forceRuntime ? { forceRuntime: merged.forceRuntime } : {}),
    message: merged.content,
    metadata: { ...merged.metadata, steer: true },
  });
};

/** Shared by streamed completion and recovery from a server terminal snapshot. */
export const scheduleQueuedFollowUp = (
  get: () => ChatStore,
  context: ConversationContext,
  operationId: string,
  onEmpty?: () => void,
): void => {
  const contextKey = messageMapKey(context);
  const execContext = { ...context };

  // Keep the queue visible until the send takes ownership of it. Recheck the
  // owner at that point: a user can start another turn during this hand-off.
  setTimeout(() => {
    if (operationSelectors.hasNewerConversationOperation(operationId, context)(get())) return;
    const remainingQueued = get().drainQueuedMessages(contextKey);
    if (remainingQueued.length === 0) {
      onEmpty?.();
      return;
    }

    sendQueuedMessages(get, execContext, remainingQueued).catch((error: unknown) => {
      console.error('[executeClientAgent] sendMessage for queued content failed:', error);
    });
  }, 100);
};
