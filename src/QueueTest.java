import org.junit.Assert;
import org.junit.Test;

public class QueueTest {
    @Test
    public void testLockFreeQueue() throws InterruptedException {
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
            q.insert(val, priority);
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
