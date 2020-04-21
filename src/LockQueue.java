public class LockQueue implements PriorityQueue {

    public LockQueue() {
    }

    @Override
    public boolean insert(Object value, Integer priority) {
        return false;
    }

    @Override
    public Object deleteMin() {
        return null;
    }

    protected class Node {
        public Object value;
        public Node next;

        public Node(Object x) {
            value = x;
            next = null;
        }
    }
}
