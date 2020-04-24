import java.util.ArrayList;
import java.util.Random;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.AtomicStampedReference;


// References
// MDList:      https://www.osti.gov/servlets/purl/1237474
//      Github: https://github.com/ucf-cs/CCSpec/blob/master/CCSpec/experiments/priority-queue/priorityqueue/mdlist/mdlist.cc

public class LockFreeQueue implements PriorityQueue {
    // TODO: Delete later
    final static boolean DEBUG = true;

    final static int DIMENSION = 8; // 8-D Linked List
    final static int R = 32;        // Threshold for physical deletions.    // TODO: Test with a lower value, like 4

    AtomicStampedReference<Node> head;    // Dummy head
    AtomicReference<Stack> del_stack;      // global deletion stack.
    AtomicReference<Stack> purging;         // flag a stack if a thread is batch purging

    public LockFreeQueue() {
        head = new AtomicStampedReference<>(new Node(0, 0), 0);
        head.getReference().purged.set(null, 1);    // Mark deleted
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
                    finishInserting(curr, curr_dim, curr_dim);
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
                        Node purged = (Node) sOld.head.getReference().purged.getReference();
                        if (purged.key < sOld.node[DIMENSION - 1].key) {
                            // Rewind stack purged
                            sNew.head.set((Node) purged.purged.getReference(), 0);
                            for (int i = 0; i < DIMENSION; i ++) {
                                sNew.node[i] = sNew.head.getReference();
                            }
                        } else {
                            break;
                        }
                    }
                    // Case: global stack head more recent than current path head
                    else {
                        Node purged = s.head.getReference().purged.getReference();
                        if (purged.key <= node.key) {
                            sNew.head.set(purged.purged.getReference(), 0);
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
                    if (del_stack.compareAndSet(sOld, sNew) || node.purged.getStamp() == 1) {
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

    private void finishInserting(Node n, int dp, int dc) {
        if (n == null) {
            return;
        }
        AdoptDesc ad = n.adesc.get();
        if (ad == null || dc < ad.pred_dim || dp > ad.curr_dim) {
            return;
        }
        Node child;
        Node curr = ad.curr;
        dp = ad.pred_dim;
        dc = ad.curr_dim;
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



    @Override
    public Object extractMin() {
        if (DEBUG) System.out.println("DELETING MIN");
        Node min = null;
        Node prg = null;
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
            AdoptDesc pending = last.adesc.get();
            if (pending != null && pending.pred_dim <= d && d < pending.curr_dim) {
                finishInserting(last, d, d);
            }
            Node child = last.child.get(d).getReference();
            if (child == null) {
                d--;
                continue;
            }
            int[] prg_stamp = new int[1];
            prg = child.purged.get(prg_stamp);
            if ((prg_stamp[0] & 1) == 1) {    // if marked as deleted
                if (prg == null) {
                    mark++;
                    for (int i = d; i < DIMENSION; i++) {
                        s.node[i] = child;
                    }
                    d = DIMENSION - 1;
                } else {                        // TODO: Should NEVER get here unless we purge
                    mark = 0;
                    s.head.set(prg, prg_stamp[0] & ~1);
//                    try {
//                        System.out.println("Here!");
//                        s.head.set((Node) val, val_stamp[0] & ~1);  // TODO: This line ALWAYS fails. WHY?
//                    } catch(Exception e) {
//                        System.out.println("FAILED TO REMOVE MIN");
//                        System.out.println("Val: " + val);
//                        System.out.println("Child key: " + child.key);
//                        System.out.println("Last key: " + last.key + ", last val: " + last.val.getReference());
//                        System.out.println("Dim: " + d);
//                        System.out.println("Stack head: " + s.head.getReference().key);
//                        System.out.println("Global stack head: " + sOld.head.getReference().key);
//                        for (Node g : s.node) {
//                            System.out.println(g.key);
//                        }
//                        System.out.println();
//                    }
                    for (int i = 0; i < DIMENSION; i++) {
                        s.node[i] = s.head.getReference();
                    }
                    d = DIMENSION - 1;
                }
            } else if (child.purged.compareAndSet(prg, prg, 0, 1)) {
                // If logical deletion succeeds, child is min
                for (int i = d; i < DIMENSION; i++) {
                    s.node[i] = child;
                }
                del_stack.compareAndExchange(sOld, s);
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
        if (min != null) return min.value;
        return -1;
    }

    /**
     * Perform physical deletions of logically deleted nodes.
     * @param hd head node of stack.
     * @param prg reference node for purge.
     */
    private void purge(AtomicStampedReference<Node> hd, Node prg) {
        if (DEBUG) System.out.println("PURGING");
        // TODO: Finish this
    }


    /**
     * Debugging function to display traversal of MDLL.
     * @param n initial node.
     * @param dim initial dimension.
     * @param prefix initially empty string.
     */
    protected static void traverseDebug(Node n, int dim, String prefix) {
        if (n.purged.getStamp() != 1) {    // is not deleted
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
        Object value;    // value
        ArrayList<AtomicStampedReference<Node>> child;  // Child nodes, stamped with ADP (stamp = 1) or PRG (stamp = 2).
        int[] coord = new int[DIMENSION];   // coordinates in 8D
        AtomicReference<AdoptDesc> adesc;  // adoption descriptor for thread helping.
        AtomicStampedReference<Node> purged;


        // Note: missing seq and purged fields, don't think it's important but add later if necessary.

        public Node(int priority, Object value) {
            key = priority;
            this.value = value;
            child = new ArrayList<>(DIMENSION);
            for (int i = 0; i < DIMENSION; i ++) {
                child.add(new AtomicStampedReference<Node>(null, 0));
            }
            adesc = new AtomicReference<>(null);
            purged = new AtomicStampedReference<>(null, 0);
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


// Possible things to try
// - atomically copy stack
// - version field instead of head node