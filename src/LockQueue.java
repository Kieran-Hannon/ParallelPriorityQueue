import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

public class LockQueue implements PriorityQueue {

    public LockQueue() {
    }

    @Override
    public boolean insert(Integer value, Integer priority) {
        return false;
    }

    @Override
    public Integer pop() {
        return null;
    }

    @Override
    public boolean is_empty() {
        return false;
    }

    protected class Node {
        public Integer value;
        public Node next;

        public Node(Integer x) {
            value = x;
            next = null;
        }
    }
}
