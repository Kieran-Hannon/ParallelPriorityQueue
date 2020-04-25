import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class LockQueue implements PriorityQueue {
    private int length;
    private Element[] A;
    private Lock[] L;
    private int last;

    public LockQueue(int length) {
        this.length = length;
        this.A = new Element[length + 1];
        this.L = new ReentrantLock[length + 1];
        this.last = 0;
        for (int i = 0; i < length + 1; i++)
            L[i] = new ReentrantLock();
    }

    public LockQueue() {
        this(1000000);
    }

    public int size() {
        return last;
    }

    @Override
    public boolean insert(Object data, Integer key) {
        lock(L[1]);
        if (last == length)  {
            unlock(L[1]);
            return false;
        }
        Element e = new Element(key, data, last + 1);
        if (last == 0) {
            e.up = false;
            A[1] = e;
            last = 1;
            unlock(L[1]);
        } else {
            lock(L[last + 1]);
            e.up = true;
            A[last + 1] = e;
            last = last + 1;
            unlock(L[1]);
            // Bubble up
            int i = e.pos;
            boolean iLocked = true;
            boolean parLocked = false;
            while (1 < i) {
                int par = parent(i);
                parLocked = tryLock(L[par]);
                if (parLocked) {
                    if (!A[par].up) {
                        if(A[i].key < A[par].key) {
                            swap(i, par);
                        } else {
                            A[i].up = false;
                            unlock(L[i]);
                            unlock(L[par]);
                            return true;
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
                    // Lock element
                    while(true) {
                        i = e.pos;
                        if (i == -1) {
                            iLocked = false;
                            break;
                        }
                        if (tryLock(L[i])) {
                            if (i == e.pos) {
                                iLocked = true;
                                break;
                            }
                            unlock(L[i]);
                        }
                    }
                    i = e.pos;
                }
            }
            e.up = false;
            if (iLocked)
                unlock(L[e.pos]);
        }
        return true;
    }

    @Override
    public Object extractMin() {
        lock(L[1]);
        Element min = A[1];
        int ls = last;
        if (ls == 0) {
            unlock(L[1]);
            return null;
        }
        A[1].pos = -1;
        if (ls == 1) {
            last = 0;
            A[1] = null;
            unlock(L[1]);
        } else {
            lock(L[ls]);
            A[1] = A[ls];
            A[1].pos = 1;
            A[ls] = null;
            last = ls - 1;
            unlock(L[ls]);
            if (ls == 2)
                unlock(L[1]);
            else {
                // Bubble down
                Element e = A[1];
                int min_pos = e.pos;
                int i;
                do {
                    i = min_pos;
                    int l = leftChild(i);
                    int r = rightChild(i);
                    if (l <= last) {
                        lock(L[l]);
                        lock(L[r]);
                        if (A[l] != null) {
                            if (A[l].key < A[i].key) {
                                min_pos = l;
                            }
                            if (A[r] != null && A[r].key < A[min_pos].key) {
                                min_pos = r;
                            }
                        }
                        if (i != min_pos) {
                            if (min_pos == l) {
                                unlock(L[r]);
                            }
                            else {
                                unlock(L[l]);
                            }
                            swap(i, min_pos);
                            unlock(L[i]);
                        } else {
                            unlock(L[l]);
                            unlock(L[r]);
                        }
                    }
                } while (i != min_pos);
                unlock(L[i]);
            }
        }
        return min.data;
    }

    private int leftChild(int i) {
        return 2 * i;
    }

    private int rightChild(int i) {
        return 2 * i + 1;
    }

    private int parent(int i) {
        return i >> 1;
    }

    private void swap(int i, int j) {
        Element temp = A[i];
        A[i] = A[j];
        A[j] = temp;
        A[i].pos = i;
        A[j].pos = j;
    }

    private void lock(Lock lock) {
        lock.lock();
    }

    private void unlock(Lock lock) {
        lock.unlock();
    }

    private boolean tryLock(Lock lock) {
        return lock.tryLock();
    }

    private static class Element {
        private Integer key;
        private Object data;

        private int pos;
        private boolean up;

        public Element(Integer key, Object data, int pos) {
            this.key = key;
            this.data = data;

            this.pos = pos;
            this.up = false;
        }
    }
}
