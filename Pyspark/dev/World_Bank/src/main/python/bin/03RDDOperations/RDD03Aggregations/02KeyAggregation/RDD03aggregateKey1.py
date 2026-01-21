# Below is a precise, step-by-step explanation of
# lambda acc, v: acc + v in the context of aggregateByKey(), 
# addressing what acc and v are and how results are stored internally.

#Where lambda acc, v: acc + v Is Used
rdd = None # Avoid error
rdd.aggregateByKey(
    0,
    lambda acc, v: acc + v,        # seqFunc
    lambda acc1, acc2: acc1 + acc2 # combFunc
)

#This lambda is the sequence function (seqFunc).

# Meaning of acc and v
# acc (Accumulator)

# Holds the running aggregated result for a specific key

# Initialized with zeroValue

# Exists per key, per partition

# v (Value)

# The current value from the RDD for that key

# Comes directly from the (key, value) pair

# Concrete Example


# Input RDD
[("a", 1), ("a", 3), ("b", 2)]
# zeroValue
0
# Execution Inside One Partition
# Key = "a"

# | Step | acc (initially 0) | v | acc + v |
# | ---- | ----------------- | - | ------- |
# | 1    | 0                 | 1 | 1       |
# | 2    | 1                 | 3 | 4       |

#Result stored internally:
("a", 4)

#Key = "b"
# | Step | acc | v | acc + v |
# | ---- | --- | - | ------- |
# | 1    | 0   | 2 | 2       |

#Stored:
("b", 2)

# How Outputs Are Stored (Important)
# Step 1: Within Each Partition

# Spark creates an in-memory map per partition:

# Partition Map:
{
  "a": 4,
  "b": 2
}

#This map stores:
# key → accumulator

#Step 2: Shuffle Phase

#Spark sends only aggregated results (not raw values):
# ("a", 4) → reducer
# ("b", 2) → reducer

# Step 3: Across Partitions (combFunc)

# If the same key appears in multiple partitions:

lambda acc1, acc2: acc1 + acc2
#Example:
# Partition 1 → ("a", 4)
# Partition 2 → ("a", 5)

# Final → ("a", 9)

# 6. Final Output Structure

# After aggregation:

# RDD[(key, aggregated_value)]

#Example:
[("a", 9), ("b", 6)]

 #Each key appears exactly once.

# 7. Visual Summary
# (key, value)
#       ↓
# acc = zeroValue
# acc = seqFunc(acc, value)   # lambda acc, v
#       ↓
# (key, partial_result)
#       ↓ shuffle
# combFunc(acc1, acc2)
#       ↓
# (key, final_result)







