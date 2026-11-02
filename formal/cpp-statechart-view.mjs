const values = value => Array.isArray(value) ? value : Object.values(value ?? {});
const list = value => `[${values(value).join(', ')}]`;

export function state(v) {
  if ('eligible' in v) {
    const phases = {
      expired: 'Lease expired', select: 'Expired lease removed', enqueue: 'No target selected',
      send: 'Target selected', record: 'Redelivery sent', done: 'Redelivery processed',
    };
    return `${phases[v.phase] ?? v.phase}; eligible ${list(v.eligible)}; ` +
      `target ${v.target}; sent ${v.sent}; pending ${v.pending}; leases ${list(v.leases)}`;
  }
  const phases = {
    subscribe: 'Subscription not sent', recognize: 'Subscription received',
    select: 'Subscription inspected', history: 'Historical replay',
    'dispatch-history': 'Historical message ready for routing',
    live: 'Historical replay complete', 'dispatch-live': 'Live message ready for routing',
    done: 'Delivery complete',
  };
  return `${phases[v.phase] ?? v.phase}; bookmark ${v.wireBookmark}; recognized ${v.recognizedBookmark}; ` +
    `selected ${list(v.pending)}; frame ${v.frame}; route ${v.sids || 'missing'}; ` +
    `received ${list(v.delivered)}`;
}

const names = {
  SendSubscribe: 'Send the subscription', RecognizeBookmark: 'Recognize the bookmark field',
  SelectHistory: 'Select earlier messages', SendHistory: 'Send the next earlier message',
  DispatchHistory: 'Route the earlier message', FinishHistory: 'Finish earlier messages',
  SendLive: 'Send the live message', DispatchLive: 'Route the live message', Done: 'Stay complete',
  RemoveExpiredLease: 'Remove the expired lease', SelectRedeliveryTarget: 'Choose a subscriber',
  EnqueuePending: 'Keep the message pending', SendRedelivery: 'Send the message again',
  RecordLease: 'Record the new lease',
};
export function step(action, from, to) {
  if (action === 'RecognizeBookmark' && to.wireBookmark !== 'plain' && to.recognizedBookmark === 'plain') {
    return 'Ignore the bookmark field';
  }
  if (action === 'EnqueuePending' && !to.pending) return 'Lose the pending message';
  return names[action] ?? action;
}
export const title = 'Bounded C++ subscription and queue delivery';
