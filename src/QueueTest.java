import org.junit.Assert;
import org.junit.Test;

public class QueueTest {
    @Test
    public void testLockFreeQueue() throws InterruptedException {
    }

    @Test
    public void testLockQueue() throws InterruptedException {
    }


    static class Insert_Thread implements Runnable {
        int val, priority;
        PriorityQueue q;
        public Insert_Thread(int val_to_enq, int priority, PriorityQueue q) {
            val = val_to_enq;
            this.q = q;
            this.priority = priority;
        }
        @Override
        public void run() {
            q.insert(val, priority);
        }
    }

    static class Remove_Thread implements Runnable {
        Integer val;
        PriorityQueue q;
        public Remove_Thread(PriorityQueue q) {
            val = null;
            this.q = q;
        }
        @Override
        public void run() {
            val = q.pop();
        }
        public int get_val() {
            return val;
        }
    }

}
