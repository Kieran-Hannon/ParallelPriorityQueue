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

        for (int i = 0; i < length + 1; i++)
            L[i] = new ReentrantLock();
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
            bubbleUp(e);
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
            else
                bubbleDown(A[1]);
        }
        return min.data;
    }

    private void bubbleUp(Element e) {
        int i = e.pos;
        boolean iLocked = true;
        boolean parLocked = false;
        while (1 < i) {
            int par = parent(i);
            parLocked = tryLock(L[par]);
            if (parLocked) {
                if (!A[par].up) {
                    if (A[i].key < A[par].key)
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
                iLocked = lockElement(e);
                i = e.pos;
            }
        }
        e.up = false;
        if (iLocked)
            unlock (L[e.pos]);
    }

    private boolean lockElement(Element e) {
        while (true) {
            int i = e.pos;
            if (i == -1)
                return false;
            if (tryLock(L[i])) {
                if (i == e.pos)
                    return true;
                unlock(L[i]);
            }
        }
    }

    private void bubbleDown(Element e) {
        int min;
        Element mp;

        int i = 1;
        Element ip = A[i];
        int left = leftChild(i);
        while (left <= last) {
            int right = left + 1;
            Element lp = A[left];
            Element rp = A[right];
            Element np;
            lock(L[left]);
            if (lp == null) {
                unlock(L[left]);
                break;
            } else {
                min = left;
                mp = lp;
            }
            if (right <= last) {
                lock(L[right]);
                np = rp;
                if (rp != null) {
                    if (rp.key < lp.key) {
                        min = right;
                        mp = rp;
                        np = lp;
                    }
                }
                unlock(L[np.pos]);
            }
            if (mp.key < ip.key)
                swap(mp.pos, ip.pos);
            else {
                unlock(L[mp.pos]);
                break;
            }
            unlock(L[ip.pos]);
            i = min;
            ip = mp;
            left = i << 1;
        }
        unlock(L[ip.pos]);
//        do {
//            i = min;
//            int l = leftChild(i);
//            int r = rightChild(i);
//            if (l <= last) {
//                lock(L[l]);
//                lock(L[r]);
//                if (A[l] != null) {
//                    if (A[l].key < A[i].key)
//                        min = l;
//                    if (A[r] != null && A[r].key < A[min].key)
//                        min = r;
//                    if (i != min) {
//                        if (i == l)
//                            unlock(L[r]);
//                        else
//                            unlock(L[l]);
//                        swap(i, min);
//                        unlock(L[i]);
//                    }
//                }
//            }
//        } while (i != min);
//        unlock(L[i]);
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

        swapLocks(i, j);
    }

    private void swapLocks(int i, int j) {
        Lock temp = L[i];
        L[i] = L[j];
        L[j] = temp;
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

    private class Element {
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
