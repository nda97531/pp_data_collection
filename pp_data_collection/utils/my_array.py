from typing import List
import numpy as np


def interval_intersection(ranges_1: List[List[int]], ranges_2: List[List[int]]) -> List[List[int]]:
    result = []
    i = 0
    j = 0

    while i < len(ranges_1) and j < len(ranges_2):
        # check if ranges_1[i] intersects ranges_2[j].

        # lo - the startpoint of the intersection
        lo = max(ranges_1[i][0], ranges_2[j][0])
        # hi - the endpoint of the intersection
        hi = min(ranges_1[i][1], ranges_2[j][1])

        if lo < hi:
            result.append([lo, hi])

        # remove the interval with the smallest endpoint
        if ranges_1[i][1] < ranges_2[j][1]:
            i += 1
        else:
            j += 1

    return result


def my_interval_intersection(intervals: List[List[List[int]]]) -> List[List[int]]:
    result = []

    # index indicating current position for each interval list
    pos_indices = np.zeros(len(intervals), dtype=int)
    # lengths of all ranges
    all_len = np.array([len(interval) for interval in intervals])

    while np.all(pos_indices < all_len):
        # check if ranges_1[i] intersects ranges_2[j].

        # the startpoint of the intersection
        lo = max([intervals[interval_idx][pos_idx][0] for interval_idx, pos_idx in enumerate(pos_indices)])
        # the endpoint of the intersection
        endpoints = [intervals[interval_idx][pos_idx][1] for interval_idx, pos_idx in enumerate(pos_indices)]
        hi = min(endpoints)

        if lo < hi:
            result.append([lo, hi])

        # remove the interval with the smallest endpoint
        pos_indices[np.argmin(endpoints)] += 1

    return result


r1 = [[0, 2], [5, 10], [13, 23], [24, 25]]
r2 = [[1, 5], [8, 12], [15, 24], [25, 26]]
r3 = [[3, 6], [5, 15], [16, 26]]

print(interval_intersection(interval_intersection(r1, r2), r3))
print(my_interval_intersection([r1, r2, r3]))
