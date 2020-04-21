import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class LockQueue implements PriorityQueue {
    private Node[] A;
    private int last;
    private int length;

    private Lock[] L;

    public LockQueue(int length) {
        this.length = length;
        this.A = new Node[length + 1];
        this.last = 0;
        this.L = new Lock[length + 1];

        for (int i = 0; i < length + 1; i++)
            this.L[i] = new ReentrantLock();
    }

    @Override
    public boolean insert(Object value, Integer priority) {
        lock(L[1]);
        if (last == length) {
            unlock(L[1]);
            return false;
        }

        Node node = new Node(value, priority, last + 1);
        if (last == 0) {
            node.up = false;
            A[1] = node;
            last = 1;
            unlock(L[1]);
        } else {
            lock(L[last + 1]);
            node.up = true;
            A[last + 1] = node;
            last++;
            unlock(L[1]);
            bubbleUp(node);
        }

        return true;
    }

    @Override
    public Object extractMin() {
        lock(L[1]);
        Node min = A[1];
        int ls = last;
        if (ls == 0) {
            unlock(L[1]);
            return null;
        }

        A[1].position = -1;
        if (ls == 1) {
            last = 0;
            A[1] = null;
            unlock(L[1]);
        } else {
            lock(L[ls]);
            A[1] = A[ls];
            A[1].position = 1;
            A[ls] = null;
            last = ls - 1;
            unlock(L[ls]);
            if (ls == 2)
                unlock(L[1]);
            else
                bubbleDown(A[1]);
        }
        return min;
    }

    private void bubbleUp(Node node) {
        int i = node.position;
        boolean iLocked = true;
        boolean parLocked = false;
        while (i > 1) {
            int par = parent(i);
            parLocked = tryLock(L[par]);
            if (parLocked) {
                if (!A[par].up) {
                    if (A[i].priority < A[par].priority)
                        swap(i, par);
                    else {
                        A[i].up = false;
                        unlock(L[i]);
                        unlock(L[par]);
                        return;
                    }
                } else {
                    unlock(L[par]);
                    parLocked = false;
                }
            }
            unlock(L[i]);
            iLocked = false;
            if (parLocked) {
                i = par;
                iLocked = true;
            } else {
                iLocked = lockElement(node);
                i = node.position;
            }
        }
        node.up = false;
        if (iLocked)
            unlock(L[node.position]);
    }

    private boolean lockElement(Node node) {
        while (true) {
            int i = node.position;
            if (i == -1)
                return false;
            if (tryLock(L[i])) {
                if (i == node.position) {
                    unlock(L[i]);
                    return true;
                }
            }
        }
    }

    private void bubbleDown(Node node) {
        int min = node.position;
        int i = -1;

        while (min != i) {
            i = min;
            int l = leftChild(i);
            int r = rightChild(i);
            if (l <= last) {
                lock(L[l]);
                lock(L[r]);
                if (A[l] != null) {
                    if (A[l].priority < A[i].priority)
                        min = l;
                    if (A[r] != null && A[r].priority < A[min].priority)
                        min = r;
                    if (i != min) {
                        if (i == l)
                            unlock(L[r]);
                        else
                            unlock(L[l]);

                        swap(i, min);
//                        unlock(L[min]);
                    }
                }
            }
        }

//        unlock(L[min]);
    }

    private int leftChild(int i) { return i * 2; }

    private int rightChild(int i) { return i * 2 + 1; }

    private int parent(int i) { return i / 2; }

    private void swap(int i, int j) {
        Node temp = A[i];
        A[i] = A[j];
        A[j] = temp;
        A[i].position = i;
        A[j].position = j;
    }

    private void lock(Lock lock) { lock.lock(); }

    private boolean tryLock(Lock lock) {
        return lock.tryLock();
    }

    private void unlock(Lock lock) {
        lock.unlock();
    }

    protected class Node {
        public Object value;
        public Integer priority;
        public Integer position;

        public boolean up;

        public Node(Object value, Integer priority, int position) {
            this.value = value;
            this.priority = priority;
            this.position = position;
        }
    }
}
