# Threaded queue processor.
class QThread
  # Takes a block to process each item.
  def initialize(&block)
    @process_item_block = block
    @queue = Thread::Queue.new
    @thread = Thread.new { worker }
  end

  # Add item to queue.
  def put(item)
    logger.debug "Adding item to the queue."
    @queue << item
  end
  alias << put

  # Clear all items from queue.
  def clear_queue
    logger.debug 'Removing all items from the queue'
    @queue.clear
  end

  private

  # Thread worker: pops and processes items.
  def worker
    while true
      item = @queue.pop
      logger.debug "Processing item from the queue."
      @process_item_block&.call(item)
    end
  end
end
