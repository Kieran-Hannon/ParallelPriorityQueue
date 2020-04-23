import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;

public class QueueTest {
    @Test
    public void testLockFreeQueueSequential() throws InterruptedException {
        // Test sequentially with ~500,000 insertions and deletions
        LockFreeQueue q = new LockFreeQueue();
        int insert_count = 1;
        for (int i = 1; i < 1000; i++) {
            int num_to_insert = 1000 - i;
            int num_to_del = i;
            while (num_to_insert > 0) {
                Assert.assertTrue(q.insert(insert_count, insert_count));
                insert_count++;
                num_to_insert--;
            }
            int last_min = 0;
            while (num_to_del > 0) {
                int min = (Integer) q.extractMin();
                System.out.println("MIN: " + min);
                Assert.assertTrue(min > last_min);
                last_min = min;
                num_to_del--;
            }
        }
        int min = (Integer) q.extractMin();
        System.out.println("MIN: " + min);
        Assert.assertEquals(min, -1);
    }

    @Test
    public void testLockFreeQueueConcurrent() throws InterruptedException {
        LockFreeQueue q = new LockFreeQueue();
        ArrayList<Thread> threads = new ArrayList<>();
        for (int i = 1; i < 200; i ++) {
            Thread t = new Thread(new Insert_Thread(i, i, q));
            t.start();
            threads.add(t);
        }
        for(Thread t:threads) {
            t.join();
        }
        threads = new ArrayList<>();
        ArrayList<Extract_Thread> extract_threads = new ArrayList<>();
        for (int i = 201; i < 300; i ++) {
            Extract_Thread e = new Extract_Thread(q);
            extract_threads.add(e);
            Thread t = new Thread(e);
            Thread t1 = new Thread(new Insert_Thread(i, i, q));
            t.start();
            t1.start();
            threads.add(t);
        }
        for(Thread t:threads) {
            t.join();
        }
        for(Extract_Thread t : extract_threads) {
            System.out.println("Min was " + t.val);
            Assert.assertTrue((Integer)t.val < 100 && (Integer)t.val > 0);
        }
        threads = new ArrayList<>();
        extract_threads = new ArrayList<>();
        for (int i = 1; i < 200; i ++) {
            Extract_Thread e = new Extract_Thread(q);
            extract_threads.add(e);
            Thread t = new Thread(e);
            t.start();
            threads.add(t);
        }
        for(Thread t:threads) {
            t.join();
        }
        for(Extract_Thread t : extract_threads) {
            System.out.println("Min was " + t.val);
            Assert.assertTrue((Integer)t.val < 300 && (Integer)t.val >= 100);
        }
        Assert.assertEquals((int) q.extractMin(), -1);


    }

    @Test
    public void testLockQueue() throws InterruptedException {
        int numberOfThreads = 10000;

        LockQueue q = new LockQueue(numberOfThreads);

        Runnable[] inserters = new Runnable[numberOfThreads];

        for (int i = 0; i < numberOfThreads; i++)
            inserters[i] = new Insert_Thread(i, i, q);

        for (int i = 0; i < numberOfThreads; i++)
            inserters[i].run();

        int[] expected = new int[numberOfThreads];

        for (int i = 0; i < numberOfThreads; i++)
            expected[i] = i;

        int[] actual = new int[numberOfThreads];

        for (int i = 0; i < numberOfThreads; i++)
            actual[i] = (Integer)((LockQueue.Node) q.extractMin()).value;

        Assert.assertArrayEquals(expected, actual);
    }

    static class Insert_Thread implements Runnable {
        Object val;
        Integer priority;
        PriorityQueue q;
        public Insert_Thread(Object val_to_enq, int priority, PriorityQueue q) {
            val = val_to_enq;
            this.q = q;
            this.priority = priority;
        }
        @Override
        public void run() {
            if (!q.insert(val, priority)) {
                System.out.println("WARNING: FAILED TO ADD " + val);
            }
        }
    }

    static class Extract_Thread implements Runnable {
        Object val;
        PriorityQueue q;
        public Extract_Thread(PriorityQueue q) {
            val = null;
            this.q = q;
        }
        @Override
        public void run() {
            val = q.extractMin();
        }
        public Object get_val() {
            return val;
        }
    }

}
