import java.util.ArrayList;
import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.AtomicStampedReference;


// References
// MDList:      https://www.osti.gov/servlets/purl/1237474
//      Github: https://github.com/ucf-cs/CCSpec/blob/master/CCSpec/experiments/priority-queue/priorityqueue/mdlist/mdlist.cc

public class LockFreeQueue implements PriorityQueue {
    // Test main, delete later
    public static void main(String[] args) {
        int[] arr = new int[8];
        int key = makeUnique(3);
        keyToCoord8(key, arr);
        System.out.println(key);
        System.out.println(Arrays.toString(arr));
        PriorityQueue q = new LockFreeQueue();
    }

    final static int DIMENSION = 8;
    AtomicStampedReference<Node> head;    // Dummy head
    AtomicReference<Stack> del_stack;      // deletion stack.

    public LockFreeQueue() {
        head = new AtomicStampedReference<>(new Node(0, 0), 0);
        del_stack = new AtomicReference<>(new Stack());
        del_stack.get().head = head;    // Set dummy head to stack head.
        for (int i = 0; i < DIMENSION; i ++) {
            del_stack.get().node[i] = head.getReference();
        }
    }


    @Override
    public boolean insert(Integer value, Integer priority) {
        // priority = makeUnique(priority);    // make priority unique using rng, TODO: uncomment later

        Stack s = new Stack();
        Node node = new Node(priority, value);
        keyToCoord8(node.key, node.coord);

        // Retry insertions until CAS successful
        while(true) {
            Node pred = null;
            int[] curr_stamp_holder = new int[1];
            Node curr = head.get(curr_stamp_holder);
            int pred_dim = 0;
            int curr_dim = 0;
            s.head = new AtomicStampedReference<>(curr, curr_stamp_holder[0]);

            // Locate Predecessor- search multidimensional LL.
            while(curr_dim < DIMENSION) {
                while (curr != null && node.coord[curr_dim] > curr.coord[curr_dim]) {
                    pred = curr;
                    pred_dim = curr_dim;
                    finishInserting(curr, pred_dim, curr_dim);
                    curr = curr.child.get(curr_dim).get(curr_stamp_holder);
                }
                if (curr == null || node.coord[curr_dim] < curr.coord[curr_dim]) break;
                else {
                    s.node[curr_dim] = curr;
                    curr_dim++;
                }
            }

            if (curr_dim == DIMENSION) return false;    // Unable to find location in 8 dimensions

            finishInserting(curr, pred_dim, curr_dim);

            // Fill new node
            if (pred_dim < curr_dim) {
                node.adesc.set(new AdoptDesc(curr, pred_dim, curr_dim));
            }
            for (int i = 0; i < pred_dim; i ++) {
                node.child.get(i).set(null, 1);     // MARK: ADP-- mark for adoption
            }
            node.child.get(curr_dim).set(curr, 0);

            // Compare and swap attempt
            if (pred.child.get(pred_dim).compareAndSet(curr, node, curr_stamp_holder[0], 0)) {
                finishInserting(node, pred_dim, curr_dim);

                // Rewind Stack- synchronize with deletions
                Stack sOld = del_stack.get();
                Stack sNew = new Stack();
                boolean first_iter = true;
                while (true) {
                    // Case: head node unchanged
                    if (s.head.getStamp() == sOld.head.getStamp()) {
                        if (node.key <= sOld.node[DIMENSION - 1].key) {
                            // Rewind stack to new node
                            int[] s_stamp = new int[1];
                            sNew.head.set(s.head.get(s_stamp), s_stamp[0]);
                            for (int i = 0; i < pred_dim; i++) {
                                sNew.node[i] = s.node[i];
                            }
                            for (int i = pred_dim; i < DIMENSION; i++) {
                                sNew.node[i] = pred;
                            }
                        } else if (first_iter) {
                            // copy sOld into sNew
                            int[] sOld_stamp = new int[1];
                            sNew.head.set(sOld.head.get(sOld_stamp), sOld_stamp[0]);
                            for (int i = 0; i < DIMENSION; i ++) {
                                sNew.node[i] = sOld.node[i];
                            }
                        } else {
                            break;
                        }
                    }
                    // Case: current path head more recent than global stack head
                    else if (s.head.getStamp() > sOld.head.getStamp()) {
                        Node purged = (Node) sOld.head.getReference().val.getReference();
                        if (purged.key < sOld.node[DIMENSION - 1].key) {
                            // Rewind stack purged
                            sNew.head.set((Node) purged.val.getReference(), 0);
                            for (int i = 0; i < DIMENSION; i ++) {
                                sNew.node[i] = sNew.head.getReference();
                            }
                        } else {
                            break;
                        }
                    }
                    // Case: global stack head more recent than current path head
                    else {
                        Node purged = (Node) s.head.getReference().val.getReference();
                        if (purged.key <= node.key) {
                            sNew.head.set((Node) purged.val.getReference(), 0);
                            for (int i = 0; i < DIMENSION; i++) {
                                sNew.node[i] = sNew.head.getReference();
                            }
                        } else {
                            // Rewind stack to new node
                            int[] s_stamp = new int[1];
                            sNew.head.set(s.head.get(s_stamp), s_stamp[0]);
                            for (int i = 0; i < pred_dim; i++) {
                                sNew.node[i] = s.node[i];
                            }
                            for (int i = pred_dim; i < DIMENSION; i++) {
                                sNew.node[i] = pred;
                            }
                        }
                    }
                    // Exit if CAS succeeds or node was deleted
                    if (del_stack.compareAndSet(sOld, sNew) || node.val.getStamp() == 1) {
                        break;
                    }
                    first_iter = false;
                    sOld = del_stack.get();
                }
                break;
            }
        }
        return true;
    }

    @Override
    public Integer deleteMin() {
        Node min = null;
        Stack sOld = del_stack.get();
        Stack s = new Stack();
        int[] sOld_stamp = new int[1];
        s.head.set(sOld.head.get(sOld_stamp), sOld_stamp[0]);
        for (int i = 0; i < DIMENSION; i++) {
            s.node[i] = sOld.node[i];
        }

        int d = DIMENSION - 1;
        while (d > 0) {
            Node last = s.node[d];
            finishInserting(last, d, d);
            AtomicStampedReference<Node> child = last.child.get(d);
            child = // ?
            if (child == null) {
                d = d - 1;
                continue;
            }
            AtomicStampedReference val = new AtomicStampedReference();
            val.set(child.getReference(), child.getStamp());
            if (val.getStamp() == 1) {
                if () { // clearmark
                    for (int i = d; i < DIMENSION; i++) {
                        s.node[i] = child.getReference();
                    }
                    d = DIMENSION - 1;
                } else {
                    s.head = //clear mark
                    for (int i = 0; i < DIMENSION; i++) {
                        s.node[i] = s.head.getReference();
                    }
                    d = DIMENSION - 1;
                }

            }
        }

        return 0;
    }

    private void purge(Node prg) {

    }

    private void finishInserting(Node n, int dp, int dc) {
        if (n == null) {
            return;
        }
        AtomicReference<AdoptDesc> ad = n.adesc;
        if (ad.get() == null || dc < ad.get().pred_dim || dp > ad.get().curr_dim) {
            return;
        }
        Node child;
        Node curr = ad.get().curr;
        dp = ad.get().pred_dim;
        dc = ad.get().curr_dim;
        for (int i = dp; i < dc; i++) {
            int[] child_stamp = new int[1];
            child = curr.child.get(i).get(child_stamp);
            // Janky replacement for FetchAndOr to atomically get and set ADP flag.
            while (!curr.child.get(i).compareAndSet(child, child, child_stamp[0], child_stamp[0] | 1)) {
                child = curr.child.get(i).get(child_stamp);
            }
            n.child.get(i).compareAndSet(null, child, 0, 0);
        }
        n.adesc = null;
    }

    /**
     * Map entire positive integer key space into 8-dimensional coordinates.
     * @param key positive integer key.
     * @param coord 8-element array where coordinates will be stored.
     */
    private static void keyToCoord8(int key, int[] coord) {
        int basis = 0x10;
        int quotient = key;
        for (int i = 7; i >= 0; i--) {
            coord[i] = quotient % basis;
            quotient /= basis;
        }
    }

    private static int makeUnique(int key) {
        // Key assumed to be below 64K
        // Chance of collision = 1/(2^15), retry if failure
        key = key << 15;
        Random r = new Random();
        key += r.nextInt(1 << 15 - 1);
        return key;
    }





    static class Node {
        // A node in multi-dimensional linked list representing key-value pair.

        int key;    // priority
        AtomicStampedReference<Object> val;    // value, marked with DEL for logical deletion (stamp = 1)
        ArrayList<AtomicStampedReference<Node>> child;  // Child nodes, stamped with ADP (stamp = 1) or PRG (stamp = 2).
        int[] coord = new int[DIMENSION];   // coordinates in 8D
        AtomicReference<AdoptDesc> adesc;  // adoption descriptor for thread helping.

        // Note: missing seq and purged fields, don't think it's important but add later if necessary.

        public Node(int priority, int value) {
            key = priority;
            val = new AtomicStampedReference<>(value, 0);
            child = new ArrayList<>(DIMENSION);
            for (int i = 0; i < DIMENSION; i ++) {
                child.add(new AtomicStampedReference<Node>(null, 0));
            }
            adesc = new AtomicReference<>(null);
        }
    }

    static class AdoptDesc {
        // Represents an unfinished adoption operation for helping other threads.

        Node curr;
        int pred_dim;
        int curr_dim;

        public AdoptDesc(Node curr, int pred_dim, int curr_dim) {
            this.curr = curr;
            this.pred_dim = pred_dim;
            this.curr_dim = curr_dim;
        }
    }

    static class Stack {
        Node[] node = new Node[DIMENSION];
        AtomicStampedReference<Node> head = new AtomicStampedReference<>(null, 0);
    }

}