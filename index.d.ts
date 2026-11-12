import type * as Y from 'yjs';
import type { Awareness } from 'y-protocols/awareness';
import type { ObservableV2 } from 'lib0/observable';

/** The one client event name the provider uses. */
export const EVENT: 'client-yjs';

export interface HibiraProviderOptions {
  /** Subscribe right away. Default true. */
  connect?: boolean;
  /** Share an existing Awareness instead of creating one. */
  awareness?: Awareness;
  /** Client events sent per second at most. Default 8 (servers allow 10). */
  maxRate?: number;
  /** Messages bigger than this are split into chunks. Default 4000 (Hibira Free allows 4 KB). */
  maxEventBytes?: number;
  /** Peers answer a newcomer after a random delay up to this many ms. Default 250. */
  answerDelay?: number;
  /** With no answer after this many ms you are alone, and synced. Default 1500. */
  syncTimeout?: number;
  /** Ms to wait for the rest of a split message. Default 30000. */
  chunkTimeout?: number;
}

export type ProviderStatus = 'connecting' | 'connected' | 'disconnected';

export interface HibiraProviderEvents {
  status: (event: { status: ProviderStatus }) => void;
  synced: (synced: boolean) => void;
  sync: (synced: boolean) => void;
  error: (error: unknown) => void;
}

/** Minimal pusher-js shape the provider needs. */
export interface PusherLike {
  subscribe(channelName: string): ChannelLike;
  unsubscribe(channelName: string): void;
  connection?: {
    bind(event: string, fn: (...args: any[]) => void): void;
    unbind(event: string, fn: (...args: any[]) => void): void;
  };
}

/** Minimal pusher-js channel shape the provider needs. */
export interface ChannelLike {
  name: string;
  subscribed: boolean;
  pusher?: PusherLike;
  bind(event: string, fn: (...args: any[]) => void): unknown;
  unbind(event: string, fn: (...args: any[]) => void): unknown;
  trigger(event: string, data: unknown): boolean;
}

export class HibiraProvider extends ObservableV2<HibiraProviderEvents> {
  /**
   * Subscribe to a private- or presence- channel and keep `doc` in sync on it.
   * Client events must be turned on for the app.
   */
  constructor(pusher: PusherLike, channelName: string, doc: Y.Doc, options?: HibiraProviderOptions);

  /**
   * Use a channel you already subscribed to, e.g. with Laravel Echo:
   * `HibiraProvider.fromChannel(Echo.private('doc.42').subscription, doc)`.
   */
  static fromChannel(channel: ChannelLike, doc: Y.Doc, options?: HibiraProviderOptions): HibiraProvider;

  readonly doc: Y.Doc;
  readonly channelName: string;
  readonly awareness: Awareness;
  /** True once the document caught up with the peers (or nobody answered). */
  readonly synced: boolean;
  readonly status: ProviderStatus;

  /** Subscribe again after disconnect(). */
  connect(): void;
  /** Tell peers you left, stop syncing and unsubscribe (if the provider subscribed). */
  disconnect(): void;
  /** disconnect() and stop listening to the document. */
  destroy(): void;
}
