import java.util.ArrayList;

public class PerformanceComparison {
    public static void main(String[] args) throws InterruptedException {
        System.out.println("**************************************************************");
        System.out.println("********************PERFORMANCE COMPARISON********************");
        System.out.println("**************************************************************\n");

        for (int num_threads = 1; num_threads <= 16; num_threads++) {
            System.out.println("Using " + num_threads + " threads\n");

            System.out.println("Beginning lock-based test");
            MyPriorityQueue lockBased = new LockQueue();
            long start = System.currentTimeMillis();
            TestQueue(lockBased, num_threads);
            long lockBasedTime = System.currentTimeMillis() - start;
            System.out.println("Lock-based queue took " + lockBasedTime + " milliseconds\n");

            System.out.println("Beginning lock-free test");
            MyPriorityQueue lockFree = new LockFreeQueue();
            start = System.currentTimeMillis();
            TestQueue(lockFree, num_threads);
            long lockFreeTime = System.currentTimeMillis() - start;
            System.out.println("Lock-free queue took " + lockFreeTime + " milliseconds\n");
        }
    }

    private static void TestQueue(MyPriorityQueue q, int num_threads) throws InterruptedException {
        int num_ops = 1000000;
        int chunk_size = num_ops/num_threads;
        int last_size = num_ops - (chunk_size*(num_threads - 1));   // for rounding errors

        ArrayList<Thread> threads = new ArrayList<>();
        int start_idx = 1;

        // Insertions only
        for (int i = 0; i < num_threads - 1; i ++) {
            Thread t = new Thread(new Insert_Thread(q, chunk_size, start_idx));
            threads.add(t);
            t.start();
            start_idx += chunk_size;
        }
        Thread t = new Thread(new Insert_Thread(q, last_size, start_idx));
        t.start();
        threads.add(t);
        start_idx += last_size;

        for (Thread thread : threads) {
            thread.join();
        }

        // Concurrent insertions and deletions
        threads = new ArrayList<>();
        for (int i = 0; i < num_threads - 1; i ++) {
            t = new Thread(new ConcurrentThread(q, chunk_size, start_idx));
            threads.add(t);
            t.start();
            start_idx += chunk_size;
        }
        t = new Thread(new ConcurrentThread(q, last_size, start_idx));
        threads.add(t);
        t.start();

        for (Thread thread : threads) {
            thread.join();
        }

        // Deletions only
        threads = new ArrayList<>();
        for (int i = 0; i < num_threads - 1; i ++) {
            t = new Thread(new Extract_Thread(q, chunk_size));
            threads.add(t);
            t.start();
        }
        t = new Thread(new Extract_Thread(q, last_size));
        threads.add(t);
        t.start();

        for (Thread thread : threads) {
            thread.join();
        }
    }


    static class Insert_Thread implements Runnable {
        MyPriorityQueue q;
        int num_insertions;
        int start_idx;
        public Insert_Thread(MyPriorityQueue q, int num_insertions, int start_idx) {
            this.q = q;
            this.num_insertions = num_insertions;
            this.start_idx = start_idx;
        }
        @Override
        public void run() {
            for (int i = start_idx; i < start_idx + num_insertions; i ++) {
                q.insert(i, i);
            }
        }
    }

    static class Extract_Thread implements Runnable {
        MyPriorityQueue q;
        int num_deletions;
        public Extract_Thread(MyPriorityQueue q, int num_deletions) {
            this.q = q;
            this.num_deletions = num_deletions;
        }
        @Override
        public void run() {
            for (int i = 0; i < num_deletions; i ++) {
                q.extractMin();
            }
        }
    }

    static class ConcurrentThread implements Runnable {
        int num;
        MyPriorityQueue q;
        int start_idx;
        public ConcurrentThread(MyPriorityQueue q, int num, int start_idx) {
            this.num = num;
            this.q = q;
            this.start_idx = start_idx;
        }
        @Override
        public void run() {
            for (int i = start_idx; i < start_idx + num; i ++) {
                q.extractMin();
                q.insert(i, i);
            }
        }
    }

}
