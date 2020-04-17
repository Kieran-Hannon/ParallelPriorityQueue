import java.util.ArrayList;
import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.atomic.AtomicMarkableReference;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.AtomicReferenceArray;
import java.util.concurrent.atomic.AtomicStampedReference;


// Reference Papers
// CBDQ:        http://www.cs.technion.ac.il/~erez/Papers/cbpq-paper-l.pdf
//      Github: https://github.com/nachshonc/ChunkBasedPriorityQueue/blob/master/ChunkedPriorityQueue.cpp
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
    }

    final static int DIMENSION = 8;

    AtomicStampedReference<Node> head = new AtomicStampedReference<>(new Node(), 0);    // Dummy head
    AtomicReference<Stack> del_stack = new AtomicReference<>(new Stack());      // deletion stack.

    public LockFreeQueue() {
        del_stack.get().head = head;    // Set dummy head to stack head.
    }



    @Override
    public boolean insert(Integer value, Integer priority) {
        // priority = makeUnique(priority);    // make priority unique using rng

        Stack s = new Stack();
        Node node = new Node(priority, value);
        keyToCoord8(node.key, node.coord);

        // Retry insertions until successful
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

            if (curr_dim == DIMENSION) break;           // Return false?
            finishInserting(curr, pred_dim, curr_dim);

            // Fill new node
            if (pred_dim < curr_dim) {
                Desc adesc = new Desc();
                adesc.curr = curr;
                adesc.pred_dim = pred_dim;
                adesc.curr_dim = curr_dim;
                node.adesc.set(adesc);
            }
            for (int i = 0; i < pred_dim; i ++) {
                node.child.get(i).set(null, 1);     // MARK: ADP-- mark for adoption
            }
            node.child.get(curr_dim).set(curr, 0);

            // Compare and swap attempt
            if (pred.child.get(pred_dim).compareAndSet(curr, node, curr_stamp_holder[0], 0)) {
                finishInserting(node, pred_dim, curr_dim);
                // Rewind Stack

                break;
            }
        }
        return true;
        // TODO:
        // - figure out what to do on first insertion when pred is null
        // - finish rewind stack and finishInserting
    }

    @Override
    public Integer deleteMin() {
        return null;
    }

    private void purge(Node prg) {

    }

    private void finishInserting(Node n, int dp, int dc) {

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
        AtomicStampedReference<Integer> val;    // value, marked with DEL for logical deletion (stamp = 1)
        ArrayList<AtomicStampedReference<Node>> child;  // Child nodes, stamped with ADP (stamp = 2) or PRG (stamp = 3).
        int[] coord = new int[DIMENSION];   // coordinates in 8D
        AtomicReference<Desc> adesc;  // adoption descriptor for thread helping.

        // Note: missing seq and purged fields, don't think it's important but add later if necessary.

        public Node(int priority, int value) {
            key = priority;
            val = new AtomicStampedReference<Integer>(value, 0);
            child = new ArrayList<>(DIMENSION);
            for (int i = 0; i < DIMENSION; i ++) {
                child.add(new AtomicStampedReference<Node>(null, 0));
            }
            adesc = new AtomicReference<>(null);
        }
    }

    static class Desc {
        // Represents an unfinished adoption operation for helping other threads.

        Node curr;
        int pred_dim;
        int curr_dim;
    }

    static class Stack {
        Node[] node = new Node[DIMENSION];
        AtomicStampedReference<Node> head = new AtomicStampedReference<Node>(null, 0);
    }

}