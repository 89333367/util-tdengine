package sunyu.util;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * 可等待的计数器，用于在多线程环境下等待所有任务完成
 *
 * @author SunYu
 */
public class AwaitableCounter {
    private final AtomicInteger count = new AtomicInteger(0);
    private final Object lock = new Object();

    /**
     * 增加计数
     */
    public void increment() {
        count.incrementAndGet();
    }

    /**
     * 减少计数
     */
    public void decrement() {
        if (count.decrementAndGet() == 0) {
            synchronized (lock) {
                lock.notifyAll();
            }
        }
    }

    /**
     * 等待归零，即使被中断也要等所有任务完成
     */
    public void awaitZero() {
        synchronized (lock) {
            while (count.get() > 0) {
                try {
                    lock.wait();
                } catch (InterruptedException e) {
                    // 清除中断标志，继续等待（不记录、不恢复、不返回）
                    Thread.interrupted(); // 清除中断状态，继续下一次循环
                }
            }
        }
    }
}