public interface PriorityQueue {
    /**
     * Insert a value into the queue with an assigned priority.
     * @param value integer value of the element.
     * @param priority priority of the element.
     * @return true if successfully added.
     */
    boolean insert(Integer value, Integer priority);

    /**
     * Pop the highest priority element from the queue.
     * @return value of highest priority element.
     */
    Integer deleteMin();

}
