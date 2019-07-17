package com.han.kafka.consumer;

import org.junit.Test;

import java.util.Arrays;

/**
 * @author: Hanl
 * @date :2019/6/5
 * @desc:
 */
public class QuickSort {

    @Test
    public void testQuickSort() {

        int arr[] = {65, 58, 95, 10, 57, 62, 13, 106, 78, 23, 85};

        System.out.println("排序前：" + Arrays.toString(arr));
        quickSort(0, arr.length - 1, arr);
        System.out.println("排序后：" + Arrays.toString(arr));
    }

    public void quickSort(int left, int right, int[] arr) {
        int pivot = 0;
        if (left < right) {
            pivot = doQuickSort(left, right, arr);
            quickSort(left, pivot - 1, arr);
            quickSort(pivot + 1, right, arr);
        }

    }

    public int doQuickSort(int left, int right, int[] arr) {
        int tmp = arr[left];
        while (left < right) {
            while (left < right && tmp < arr[right]) {
                right--;
            }
            arr[left] = arr[right];

            while (left < right && tmp > arr[left]) {
                left++;
            }
            arr[right] = arr[left];
        }
        arr[left] = tmp;
        return left;
    }
}
