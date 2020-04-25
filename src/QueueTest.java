import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashSet;

public class QueueTest {
    @Test
    public void testLockFreeReverseInserts() {
        LockFreeQueue q = new LockFreeQueue();
        for (int i = 1000; i > 0; i --) {
            q.insert(i, i);
        }
        int min = 0;
        for (int i = 1000; i > 0; i --) {
            int local_min = (Integer) q.extractMin();
            Assert.assertTrue(local_min > min);
            min = local_min;
        }
        Assert.assertNull(q.extractMin());
    }


    @Test
    public void testLockFreeQueueSequential() {
        LockFreeQueue q = new LockFreeQueue();
        int insert_count = 1;
        for (int i = 1; i < 100; i++) {
            int num_to_insert = 100 - i;
            int num_to_del = i;
            while (num_to_insert > 0) {
                Assert.assertTrue(q.insert(insert_count, insert_count));
                insert_count++;
                num_to_insert--;
            }
            int last_min = 0;
            while (num_to_del > 0) {
                int min = (Integer) q.extractMin();
                Assert.assertTrue(min > last_min);
                last_min = min;
                num_to_del--;
            }
        }
        Assert.assertNull(q.extractMin());
    }

    @Test
    public void testLockFreeQueueInsert() {
        int numberOfThreads = 1000;
        MyPriorityQueue q = new LockFreeQueue();
        Runnable[] inserters = new Runnable[numberOfThreads];
        for (int i = 1; i < numberOfThreads; i++)
            inserters[i] = new Insert_Thread(i, i, q);
        for (int i = 1; i < numberOfThreads; i++)
            inserters[i].run();
        int[] expected = new int[numberOfThreads];
        for (int i = 1; i < numberOfThreads; i++)
            expected[i] = i;
        int[] actual = new int[numberOfThreads];
        for (int i = 1; i < numberOfThreads; i++)
            actual[i] = (Integer)(q.extractMin());
        Assert.assertArrayEquals(expected, actual);
    }

    @Test
    public void testLockFreeQueueDelete() {
        int numberOfThreads = 1000;
        MyPriorityQueue q = new LockFreeQueue();
        for (int i = 0; i < numberOfThreads; i++)
            q.insert(i, i);
        Runnable[] deleters = new Runnable[numberOfThreads];
        for (int i = 0; i < numberOfThreads; i++)
            deleters[i] = new Extract_Thread(q);
        for (int i = 0; i < numberOfThreads; i++)
            deleters[i].run();
        Assert.assertNull(q.extractMin());
    }

    @Test
    public void testLockFreeQueueConcurrent() throws InterruptedException {
        // Stage 1: Concurrent inserts
        HashSet<Integer> set = new HashSet<>();
        for (int i = 1; i < 300; i++) {
            set.add(i);
        }
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

        // Stage 2: Concurrent inserts and deletes
        threads = new ArrayList<>();
        ArrayList<Extract_Thread> extract_threads = new ArrayList<>();
        for (int i = 200; i < 300; i ++) {
            Extract_Thread e = new Extract_Thread(q);
            extract_threads.add(e);
            Thread t = new Thread(e);
            Thread t1 = new Thread(new Insert_Thread(i, i, q));
            t.start();
            t1.start();
            threads.add(t);
            threads.add(t1);
        }
        for(Thread t:threads) {
            t.join();
        }
        for(Extract_Thread t : extract_threads) {
            Assert.assertTrue((Integer)t.val <= 100 && (Integer)t.val > 0);
            if (set.contains(t.val)) {
                set.remove(t.val);
            }
        }

        // Stage 3: concurrent deletes only
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
            Assert.assertTrue((Integer)t.val < 300 && (Integer)t.val > 100);
            set.remove(t.val);
        }
        Assert.assertNull(q.extractMin());
        for (Integer i : set) {
            System.out.println("Failed to pop " + i);
        }
        Assert.assertTrue(set.isEmpty());
    }

    @Test
    public void testLockQueueSequential() {
        int numberOfElements = 50;
        LockQueue q = new LockQueue(numberOfElements);
        for (int i = 0; i < numberOfElements; i++)
            q.insert(i, i);
        int[] expected = new int[numberOfElements];
        for (int i = 0; i < numberOfElements; i++)
            expected[i] = i;
        int[] actual = new int[numberOfElements];
        for (int i = 0; i < numberOfElements; i++)
            actual[i] = (Integer)(q.extractMin());
        Assert.assertArrayEquals(expected, actual);
    }

    @Test
    public void testLockQueueInsert() {
        int numberOfThreads = 100;
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
            actual[i] = (Integer)(q.extractMin());
        Assert.assertArrayEquals(expected, actual);
    }

    @Test
    public void testLockQueueDelete() {
        int numberOfThreads = 100;
        LockQueue q = new LockQueue(numberOfThreads);
        for (int i = 0; i < numberOfThreads; i++)
            q.insert(i, i);
        Runnable[] deleters = new Runnable[numberOfThreads];
        for (int i = 0; i < numberOfThreads; i++)
            deleters[i] = new Extract_Thread(q);
        for (int i = 0; i < numberOfThreads; i++)
            deleters[i].run();
        Assert.assertEquals(0, q.size());
    }

    @Test
    public void testLockQueueConcurrent() throws InterruptedException {
        // Stage 1: Concurrent inserts
        HashSet<Integer> set = new HashSet<>();
        for (int i = 1; i < 300; i++) {
            set.add(i);
        }
        LockQueue q = new LockQueue();
        ArrayList<Thread> threads = new ArrayList<>();
        for (int i = 1; i < 200; i ++) {
            Thread t = new Thread(new Insert_Thread(i, i, q));
            t.start();
            threads.add(t);
        }
        for(Thread t:threads) {
            t.join();
        }

        // Stage 2: Concurrent inserts and deletes
        threads = new ArrayList<>();
        ArrayList<Extract_Thread> extract_threads = new ArrayList<>();
        for (int i = 200; i < 300; i ++) {
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
            Assert.assertTrue((Integer)t.val <= 100 && (Integer)t.val > 0);
            if (set.contains(t.val)) {
                set.remove(t.val);
            }
        }

        // Stage 3: concurrent deletes only
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
            Assert.assertTrue((Integer)t.val < 300 && (Integer)t.val > 100);
            set.remove(t.val);
        }
        Assert.assertNull(q.extractMin());
        for (Integer i : set) {
            System.out.println("Failed to pop " + i);
        }
        Assert.assertTrue(set.isEmpty());
    }



    static class Insert_Thread implements Runnable {
        Object val;
        Integer priority;
        MyPriorityQueue q;
        public Insert_Thread(Object val_to_enq, int priority, MyPriorityQueue q) {
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
        MyPriorityQueue q;
        public Extract_Thread(MyPriorityQueue q) {
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
