import org.junit.Assert;
import org.junit.Test;
import org.junit.jupiter.api.Test;

public class QueueTest {
    @Test
    public void testLockFreeQueue() throws InterruptedException {
    }

    @Test
    public void testLockQueue() throws InterruptedException {
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

    static class Remove_Thread implements Runnable {
        Object val;
        PriorityQueue q;
        public Remove_Thread(PriorityQueue q) {
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
