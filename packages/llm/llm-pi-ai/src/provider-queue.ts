function abortReason(signal: AbortSignal): Error {
  return signal.reason instanceof Error
    ? signal.reason
    : new Error('Provider request aborted', { cause: signal.reason })
}

/** FIFO, cancellation-aware admission for one provider route's active request. */
export class ProviderRequestQueue {
  private readonly routes = new Map<string, { active: boolean; waiters: Waiter[] }>()

  /**
   * Wait for the provider slot and return its queue wait plus an idempotent release function.
   * A queued caller's abort signal removes its waiter; Error reasons are preserved and other reasons are wrapped.
   * @param provider - the provider route whose requests share one slot.
   * @param signal - optional caller cancellation signal.
   * @returns the elapsed queue wait and the function that releases or hands off the slot.
   */
  acquire(provider: string, signal?: AbortSignal): Promise<{ waitMs: number; release: () => void }> {
    if (signal?.aborted) return Promise.reject(abortReason(signal))
    const state = this.routes.get(provider) ?? { active: false, waiters: [] }
    this.routes.set(provider, state)
    if (!state.active) {
      state.active = true
      return Promise.resolve(this.lease(provider, state, 0))
    }
    return new Promise((resolve, reject) => {
      const waiter: Waiter = { queuedAt: performance.now(), resolve, reject, ...(signal === undefined ? {} : { signal }) }
      if (signal !== undefined) {
        waiter.onAbort = () => {
          const index = state.waiters.indexOf(waiter)
          state.waiters.splice(index, 1)
          reject(abortReason(signal))
        }
        signal.addEventListener('abort', waiter.onAbort, { once: true })
      }
      state.waiters.push(waiter)
    })
  }

  private lease(
    provider: string,
    state: { active: boolean; waiters: Waiter[] },
    waitMs: number,
  ): { waitMs: number; release: () => void } {
    let released = false
    return {
      waitMs,
      release: () => {
        if (released) return
        released = true
        while (state.waiters.length > 0) {
          const waiter = state.waiters.shift() as Waiter
          if (waiter.signal !== undefined && waiter.onAbort !== undefined) {
            waiter.signal.removeEventListener('abort', waiter.onAbort)
          }
          waiter.resolve(this.lease(provider, state, Math.max(0, performance.now() - waiter.queuedAt)))
          return
        }
        state.active = false
        this.routes.delete(provider)
      },
    }
  }
}

interface Waiter {
  queuedAt: number
  resolve: (lease: { waitMs: number; release: () => void }) => void
  reject: (reason: unknown) => void
  signal?: AbortSignal
  onAbort?: () => void
}
