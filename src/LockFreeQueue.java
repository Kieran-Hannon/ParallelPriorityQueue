import java.util.ArrayList;
import java.util.Random;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.AtomicStampedReference;


// References
// MDList:      https://www.osti.gov/servlets/purl/1237474
//      Github: https://github.com/ucf-cs/CCSpec/blob/master/CCSpec/experiments/priority-queue/priorityqueue/mdlist/mdlist.cc

public class LockFreeQueue implements PriorityQueue {
    // Test main, delete later
    final static boolean DEBUG = true;
    public static void main(String[] args) {
        LockFreeQueue q = new LockFreeQueue();
        for (int i = 1024; i >= 10; i >>= 1) {
            q.insert(i, i);
        }
        for (int i = 1; i < 10; i++) {
            q.insert(i, i);
        }
        traverseDebug(q.head.getReference(), 0, "");
        System.out.println("MIN: " + q.extractMin());
        traverseDebug(q.head.getReference(), 0, "");
        System.out.println("MIN: " + q.extractMin());
        traverseDebug(q.head.getReference(), 0, "");
        System.out.println("MIN: " + q.extractMin());
        traverseDebug(q.head.getReference(), 0, "");
        System.out.println("MIN: " + q.extractMin());
        traverseDebug(q.head.getReference(), 0, "");
        System.out.println("MIN: " + q.extractMin());
        traverseDebug(q.head.getReference(), 0, "");
        System.out.println("MIN: " + q.extractMin());
        traverseDebug(q.head.getReference(), 0, "");
        System.out.println("MIN: " + q.extractMin());
        traverseDebug(q.head.getReference(), 0, "");
        System.out.println("MIN: " + q.extractMin());
        traverseDebug(q.head.getReference(), 0, "");


    }

    final static int DIMENSION = 8; // 8-D Linked List
    final static int R = 32;        // Threshold for physical deletions. TODO: might need to set below DIMENSION?

    AtomicStampedReference<Node> head;    // Dummy head
    AtomicReference<Stack> del_stack;      // deletion stack.
    AtomicReference<Stack> purging;

    public LockFreeQueue() {
        head = new AtomicStampedReference<>(new Node(0, 0), 0);
        del_stack = new AtomicReference<>(new Stack());
        del_stack.get().head = head;    // Set dummy head to stack head.
        for (int i = 0; i < DIMENSION; i ++) {
            del_stack.get().node[i] = head.getReference();
        }
        purging = new AtomicReference<>(null);
    }


    @Override
    public boolean insert(Object value, Integer priority) {

        // priority = makeUnique(priority);    // make priority unique using rng, TODO: uncomment later
        if (DEBUG) System.out.println("INSERTING- key=" + priority + ", value=" + value);

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
    public Object extractMin() {
        if (DEBUG) System.out.println("DELETING MIN");
        Node min = null;
        Stack sOld = del_stack.get();
        int mark = 0;

        // Make local copy of global stack
        Stack s = new Stack();
        int[] sOld_stamp = new int[1];
        s.head.set(sOld.head.get(sOld_stamp), sOld_stamp[0]);
        for (int i = 0; i < DIMENSION; i++) {
            s.node[i] = sOld.node[i];
        }

        int d = DIMENSION - 1;
        while (d >= 0) {
            Node last = s.node[d];
            finishInserting(last, d, d);
            Node child = last.child.get(d).getReference();
            if (child == null) {
                d--;
                continue;
            }
            int[] val_stamp = new int[1];
            Object val = child.val.get(val_stamp);
            if (val_stamp[0] != 0) {    // if marked as deleted
                if (val == null) {
                    mark++;
                    for (int i = d; i < DIMENSION; i++) {
                        s.node[i] = child;
                    }
                } else {
                    mark = 0;
                    s.head.set((Node) val, val_stamp[0] & ~1);
                    for (int i = 0; i < DIMENSION; i++) {
                        s.node[i] = s.head.getReference();
                    }
                }
                d = DIMENSION - 1;
            } else if (child.val.compareAndSet(val, val, val_stamp[0], val_stamp[0] | 1)) {
                // If logical deletion succeeds, child is min
                for (int i = d; i < DIMENSION; i++) {
                    s.node[i] = child;
                }
                if (!del_stack.compareAndSet(sOld, s)) {
                    return -1;  // TODO: what to do if this CAS fails?
                }
                if (mark > R && purging.get() == null) {
                    // Physical deletions
                    if (purging.compareAndSet(null, s)) {
                        if (s.head == head) {
                            purge(s.head, s.node[DIMENSION - 1]);
                        }
                        purging.set(null);  // Done purging.
                    }
                }
                min = child;
                break;
            }

        }
        if (min != null) return min.val.getReference();
        return -1;
    }

    /**
     * Perform physical deletions of logically deleted nodes.
     * @param hd head node of stack.
     * @param prg reference node for purge.
     */
    private void purge(AtomicStampedReference<Node> hd, Node prg) {
        if (DEBUG) System.out.println("PURGING");
        if (hd != head) return;
        Node dummy = new Node(0, null);
        dummy.val.set(null, 1);     // mark deleted
        AtomicStampedReference<Node> hd_new = new AtomicStampedReference<>(dummy, head.getStamp() + 1);

        Node purge_cpy = new Node(prg.key, prg.val.getReference());
        purge_cpy.coord = prg.coord.clone();
        purge_cpy.adesc.set(prg.adesc.get());

        int purged_dim = -1;
        Node pvt = head.getReference();
        int dim = 0;
        while (dim < DIMENSION) {

            // Locate pivot
            boolean loc_pivot = false;
            int[] child_stamp = new int[1];
            Node child;
            while (prg.coord[dim] > pvt.coord[dim]) {
                finishInserting(pvt, dim, dim);
                pvt = pvt.child.get(dim).getReference();
            }
            while(true) {
                child = pvt.child.get(dim).get(child_stamp);
                if (pvt.child.get(dim).compareAndSet(child, child, child_stamp[0], child_stamp[0] | 2))
                    break;
                if (child != null && child_stamp[0] != 0)
                    break;
            }
            if (pvt.child.get(dim).getStamp() > 0) loc_pivot = true;

            if (!loc_pivot) {
                pvt = head.getReference();
                dim = 0;
                continue;
            }

            if (pvt == hd.getReference()) {
                hd_new.getReference().child.get(dim).set(child, 0);
                purge_cpy.child.get(dim).set(null, 1);
            } else {
                purge_cpy.child.get(dim).set(child, 0);
                if (dim == 0 || purge_cpy.child.get(dim - 1).getStamp() == 1) {
                    hd_new.getReference().child.get(dim).set(purge_cpy, 0);
                }
            }
            dim++;
        }
        hd.getReference().val.set(prg, 1);
        prg.val.set(hd_new, 1);
        head = hd_new;
    }

    /**
     * Helper function for threads helping other incomplete insertions.
     * @param n node to help.
     * @param dp predecessor dimension.
     * @param dc current node dimension.
     */
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
        n.adesc.set(null);
    }

    /**
     * Debugging function to display traversal of MDLL.
     * @param n initial node.
     * @param dim initial dimension.
     * @param prefix initially empty string.
     */
    protected static void traverseDebug(Node n, int dim, String prefix) {
        if (n.val.getStamp() != 1) {    // is not deleted
            System.out.print(prefix);
            System.out.println("Node- key:" + n.key + ", dim " + dim);
        }
        for (int i = DIMENSION - 1; i >= dim; i --) {
            if (n.child.get(i).getReference() != null) {
                prefix += '|';
                for (int j = 0; j < 8; j++) {
                    prefix += '-';
                }
                traverseDebug(n.child.get(i).getReference(), i, prefix);
                prefix = prefix.substring(0, prefix.length() - 9);
            }
        }
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

    /**
     * Function to make a unique function from a priority key.
     * @param key priority < ~64k.
     * @return randomly unique key.
     */
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

        public Node(int priority, Object value) {
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

// TODO:
// is it possible to copy entire stack atomically? might be important in both delete and insert.
// if this causes issues, come back and ensure consistency after every global stack copy, and yield otherwise