import { describe, expect, it } from 'vitest'
import { ProviderRequestQueue } from '../src/provider-queue.ts'

describe('ProviderRequestQueue', () => {
  it('hands a route slot to queued callers in FIFO order and releases it once', async () => {
    const queue = new ProviderRequestQueue()
    const first = await queue.acquire('deepseek')
    const controller = new AbortController()
    const order: number[] = []
    const secondPromise = queue.acquire('deepseek').then((lease) => { order.push(2); return lease })
    const thirdPromise = queue.acquire('deepseek', controller.signal).then((lease) => { order.push(3); return lease })

    await Promise.resolve()
    expect(order).toEqual([])
    first.release()
    const second = await secondPromise
    expect(order).toEqual([2])
    expect(second.waitMs).toBeGreaterThanOrEqual(0)
    second.release()
    const third = await thirdPromise
    expect(order).toEqual([2, 3])
    third.release()
    third.release()
    const idle = await queue.acquire('deepseek')
    expect(idle.waitMs).toBe(0)
    idle.release()
  })

  it('removes an aborted waiter without blocking the next caller', async () => {
    const queue = new ProviderRequestQueue()
    const first = await queue.acquire('deepseek')
    const controller = new AbortController()
    const cancelled = queue.acquire('deepseek', controller.signal)
    const next = queue.acquire('deepseek')
    controller.abort(new Error('cancelled while queued'))

    await expect(cancelled).rejects.toThrow('cancelled while queued')
    first.release()
    const lease = await next
    lease.release()
  })

  it('does not make different provider routes wait for each other', async () => {
    const queue = new ProviderRequestQueue()
    const first = await queue.acquire('deepseek')
    const second = await queue.acquire('openai')
    expect(second.waitMs).toBe(0)
    second.release()
    first.release()
  })

  it('rejects a caller whose signal was already aborted', async () => {
    const queue = new ProviderRequestQueue()
    const controller = new AbortController()
    controller.abort('already cancelled')
    await expect(queue.acquire('deepseek', controller.signal)).rejects.toMatchObject({
      message: 'Provider request aborted', cause: 'already cancelled',
    })

    const withoutReason = new AbortController()
    withoutReason.abort()
    await expect(queue.acquire('deepseek', withoutReason.signal)).rejects.toBeInstanceOf(DOMException)
  })

  it('uses the abort signal reason for a queued cancellation', async () => {
    const queue = new ProviderRequestQueue()
    const first = await queue.acquire('deepseek')
    const controller = new AbortController()
    const waiting = queue.acquire('deepseek', controller.signal)
    controller.abort()
    await expect(waiting).rejects.toBeInstanceOf(DOMException)
    first.release()
  })
})
