import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicStampedReference;

public class LockFreeQueue implements PriorityQueue {

    public LockFreeQueue() {
    }

    @Override
    public boolean insert(Integer value, Integer priority) {
        return false;
    }

    @Override
    public Integer deleteMin() {
        return null;
    }

    protected class Node {
        public Integer value;
        public LockQueue.Node next;

        public Node(Integer x) {
            value = x;
            next = null;
        }
    }
}