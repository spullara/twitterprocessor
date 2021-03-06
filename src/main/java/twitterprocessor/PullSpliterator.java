package twitterprocessor;

import java.util.List;
import java.util.Spliterator;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.stream.Stream;

/**
 * Split a stream by round robining the elements.
 * User: sam
 * Date: 2/21/13
 * Time: 6:54 PM
 * To change this template use File | Settings | File Templates.
 */
public class PullSpliterator<T> implements Spliterator<T> {

  // Should be much bigger than the number of elements a thread can process in 1 ms.
  private static final int BUFFER_SIZE = 10000;

  // Maximum number of processor threads
  private final int maxqueues;

  // We share the list of queues and need access to the done flag
  private final PullSpliterator<T> parent;

  // This spliterators queue of elements to process
  private final BlockingQueue<T> queue;

  // The list of queues that are being processed
  private final List<BlockingQueue<T>> queues;

  // This is set to true when there are no more elements to enqueue from the stream
  private volatile boolean done;

  /**
   * Converts a embarrassingly sequential stream into a parallel stream with
   * Streams.parallelStream(new PullSpliterator<>(stream))
   * <p/>
   * Uses the available processors as an estimate for the number of queues to generate.
   * You should experiment with this for your workload though.
   *
   * @param stream
   */
  public PullSpliterator(Stream<T> stream) {
    // Default to assuming hyperthreaded CPUs which are the most common at this point
    this(stream, Runtime.getRuntime().availableProcessors() / 2);
  }

  /**
   * Converts a embarrassingly sequential stream into a parallel stream with
   * Streams.parallelStream(new PullSpliterator<>(stream))
   *
   * @param stream    The sequential stream to convert
   * @param maxqueues The number of spliterators to generate for this workload
   */
  public PullSpliterator(Stream<T> stream, int maxqueues) {
    this.maxqueues = maxqueues;
    queues = new CopyOnWriteArrayList<>();
    queue = new ArrayBlockingQueue<>(BUFFER_SIZE);
    queues.add(queue);
    parent = this;
    ForkJoinPool.commonPool().execute(() -> {
      AtomicInteger current = new AtomicInteger();
      stream.forEach(element -> {
        if (element != null) {
          try {
            // Uneven queues can cause put to block
            // with little chance of recovery
            BlockingQueue<T> queue;
            boolean finished;
            int spin = 0;
            do {
              int size = queues.size();
              queue = queues.get(current.getAndIncrement() % size);
              if (spin++ > size) {
                // If they are all full, no need to spin, just
                // wait on the latest queue
                finished = true;
                queue.put(element);
              } else {
                finished = queue.offer(element);
              }
            } while (!finished);
          } catch (Throwable e) {
            e.printStackTrace();
          }
        }
      });
      done = true;
    });
  }

  // Creates a new split and adds itself to the parent
  private PullSpliterator(List<BlockingQueue<T>> queues, int maxqueues, PullSpliterator<T> parent) {
    this.queues = queues;
    this.maxqueues = maxqueues;
    this.parent = parent;
    this.queue = new ArrayBlockingQueue<>(BUFFER_SIZE);
    queues.add(queue);
  }

  /**
   * Spin on the queue until something shows up in it.
   *
   * @param action Call accept on this when present
   * @return false if nothing left, true if you processed something
   */
  @Override
  public boolean tryAdvance(Consumer<? super T> action) {
    int sleep = 1;
    while (true) {
      T poll = queue.poll();
      if (poll != null) {
        action.accept(poll);
        return true;
      }
      if (parent.done) return false;
      // Sadly this is way faster than poll(timeout) ~ 7% in my tests
      // Automatically adjusts for the ratio between source speed
      // and the speed at which we can process them
      try {
        Thread.sleep(sleep++);
      } catch (InterruptedException e) {
        // ignore
      }
    }
  }

  @Override
  public int characteristics() {
    return IMMUTABLE | NONNULL;
  }

  @Override
  public Spliterator<T> trySplit() {
    if (queues.size() < maxqueues) {
      return new PullSpliterator<>(queues, maxqueues, parent);
    }
    return null;
  }

  @Override
  public long estimateSize() {
    return Long.MAX_VALUE;
  }

}
