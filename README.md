# ParallelPriorityQueue
EE 361C Final Project
Juan Paez, Kieran Hannon, Zachary Chilton, and Ankur Kaushik

## Task:
Implement fine grained lock-based and lock-free priority queue algorithms. Compare the performance of lock-based and lock-free algorithms.

## Fine Grained Lock-Based Priority Queue: LockQueue.java
Our implementation of the fine-grained lock priority queue is based on the following paper by Tamir, Morrison, and Rinetzky:
https://www.cs.tau.ac.il/~mad/publications/opodis2015-heap.pdf

It uses the classical heap structure, locking only nodes being compared/swapped during heapify operations. We assign
a reentrant lock to each element and lock it each time the element is being modified during an insertion or deletion.
The primary bottleneck occurs during deletions, where many nodes contend for the lock of the root node in the heap.

## Lock Free Priority Queue: LockFreeQueue.java
Our implementation of the lock-free priority queue is based on the following paper by Zhang and Dechev:
https://www.osti.gov/servlets/purl/1237474

It consists of a multi-dimensional linked-list that is used to store priorities and values.
The delete operation uses a deletion stack to hint at the position of the next smallest node. It also differentiates
logical deletions from physical deletions in order to "delete" a node without worrying about another thread getting
stuck there.
The insert operation computes the target coordinates of the new node to be inserted and finds its predecessor.
It then uses pointer swinging to atomically insert the node at the proper location. The notion of child adoption is used
to help other threads finish incomplete insertions.
Since the algorithm only supports unique values, we have also provided a makeUnique function that uses a random-number
generator to convert a non-unique 16-bit integer into a unique 31-bit unsigned integer. Note also that the priority
of 0 is reserved for the head value.

## The Interface
Both of our queues implement the following interface:
```Java
public interface PriorityQueue {
    /**
     * Insert a value into the queue with an assigned priority.
     * @param value integer value of the element.
     * @param priority priority of the element.
     * @return true if successfully added.
     */
    boolean insert(Object value, Integer priority);

    /**
     * Pop the highest priority element from the queue.
     * @return value of highest priority element.
     */
    Object extractMin();
}
```
So to use instances of our queues, q.insert(value, priority) and min = q.extractMin() will work for both implementations.
We also demonstrate sequential and concurrent uses and tests in QueueTest.java using the JUnit framework.

## Performance Comparison:
We provide a performance comparison script in PerformanceComparison.java. If you would like to run it yourself, simply
compile and execute src/PerformanceComparison.java, which runs the performance comparison with 1 to 16 threads.

The performance comparison operates in three sequential stages. Each stage's work is split among num_threads threads.
1. Concurrently insert 1,000,000 elements.
2. Concurrently insert AND delete 1,000,000 elements.
3. Concurrently delete 1,000,000 elements.

Results of performance comparison a 4-core machine:
![Results](https://github.com/juanpaez22/ParallelPriorityQueue/blob/master/results/Parallel%20Priority%20Queue%20Results.JPG)
