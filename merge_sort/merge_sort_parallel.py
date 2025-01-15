from concurrent.futures import ProcessPoolExecutor
import multiprocessing

def merge_sort_parallel(arr):
    if len(arr) <= 1:
        return arr
    
    # Find the middle point
    mid = len(arr) // 2

    # Use process pool for parallel execution
    with ProcessPoolExecutor() as executor:
        # Submit parallel tasks
        left_future = executor.submit(merge_sort_parallel, arr[:mid])
        right_future = executor.submit(merge_sort_parallel, arr[mid:])
        
        # Wait for results
        left_half = left_future.result()
        right_half = right_future.result()
    
    # Merge the sorted halves
    return merge(left_half, right_half)

def merge(left, right):
    sorted_arr = []
    i = j = 0
    
    # Merge two sorted arrays
    while i < len(left) and j < len(right):
        if left[i] < right[j]:
            sorted_arr.append(left[i])
            i += 1
        else:
            sorted_arr.append(right[j])
            j += 1
    
    # Add any remaining elements
    sorted_arr.extend(left[i:])
    sorted_arr.extend(right[j:])
    
    return sorted_arr

# Example usage
if __name__ == "__main__":
    # Set the multiprocessing start method for compatibility
    multiprocessing.set_start_method('fork')
    
    arr = [38, 27, 43, 3, 9, 82, 10]
    print("Original array:", arr)
    sorted_arr = merge_sort_parallel(arr)
    print("Sorted array:", sorted_arr)
