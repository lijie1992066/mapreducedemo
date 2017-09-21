package com.lijie.rpcDemo;
import java.util.ArrayList;  
import java.util.Arrays;  
import java.util.Collections;  
  
public class ShortJobFirst {  
  
    public static float minAgerageWaitTime(int[] requestTime, int[] durationTime){  
          
        int length = requestTime.length;  
          
        int[] serviceTime = new int[length];  //服务时间 即是持续时间  
        for(int i=0; i<length; i++){  
            serviceTime[i] = durationTime[i];    
        }  
          
        int[] task = new int[length];  //任务号  
        for(int i=0; i<length; i++){  
            task[i] = i+1;  
        }  
        int[] waitTime = new int[length];  //等待时间  
        int[] startTime = new int[length]; //开始时间  
        int[] finishTime = new int[length]; //完成时间  
        int[] turnTime = new int[length];   //周转时间  
        int[] rightTurnTime = new int[length];  //带权周转时间  
          
        startTime[0] = requestTime[0];  
        finishTime[0] = startTime[0] + durationTime[0];  
        waitTime[0] = startTime[0] - requestTime[0];  
        turnTime[0] = finishTime[0] - requestTime[0];  
        rightTurnTime[0] = turnTime[0] / durationTime[0];  
          
        int minIndex = 0;  
        int lastIndex = 0;  
        int[] durations =  getMin( requestTime,serviceTime);  //得到任务调动的顺序  
        for(int i=1; i<length; i++){  
            minIndex = durations[i-1]+1;  
            startTime[minIndex] = finishTime[lastIndex];   //开始时间 = task == 0 ? 0 : 上个优选任务 完成时间   
            finishTime[minIndex] = startTime[minIndex] + durationTime[minIndex]; //完成时间 = 开始时间 + 服务时间  
            waitTime[minIndex] = startTime[minIndex] - requestTime[minIndex];    //等待时间 = 开始时间 - 提交时间  
            turnTime[minIndex] = finishTime[minIndex] - requestTime[minIndex];   //周转时间 = 完成时间 - 提交时间  
            rightTurnTime[minIndex] = turnTime[minIndex] / durationTime[minIndex];  //带权周转时间 = 周转时间 / 服务时间  
            lastIndex = minIndex;  //将当前索引记为上一个任务索引  
        }  
          
        //打印  
        System.out.println();  
        prints("作业名：",task);  
        prints("提交时间：",requestTime);  
        prints("服务时间：",durationTime);  
        prints("开始时间：",startTime);  
        prints("完成时间：",finishTime);  
        prints("等待时间：",waitTime);  
        prints("周转时间：",turnTime);  
        prints("带权周转时间：",rightTurnTime);  
          
        int add = 0;  
        float result;     
        for(int i=0; i< length; i++){  
            add += waitTime[i];  
        }  
        result = add/length;  //求平均等待时间  
          
        return result;  
    }  
      
    /** 
     * 打印 
     * @param name 
     * @param arr 
     */  
    private static void prints(String name, int[] arr) {  
  
        System.out.print(name);  
        for(int i=0 ; i<arr.length; i++){  
            System.out.print(arr[i] + " ");  
        }  
        System.out.println();  
          
    }  
  
    /** 
     * 得到任务调动的顺序 
     * @param requstTime 
     * @param durationTime 
     * @return 
     */  
    private static int[] getMin(int[] requstTime, int[] durationTime) {  
          
        int length = durationTime.length;  
        int[] arr = new int[length-1];  //去除第一个任务，剩下的任务的服务时间  
        int[] arr1 = new int[length-1]; //存放剩下任务的开始顺序索引  
        int[] arr2 = new int[length-1]; //存放剩下任务的开始顺序值  
        for(int i=0; i<arr.length ; i++){  
            arr[i] = durationTime[i+1];  
        }  
          
        int minIndex =0;    
        for(int i=0; i<arr.length; i++){  //趟数  
            for(int j=0 ; j<arr.length; j++){  //冒泡比较法，但是不交换位置  
                if(arr[j] < arr[minIndex]){  
                    minIndex = j;  
                }  
            }  
            arr1[i] = minIndex;  
            arr2[i] = arr[minIndex];  
            arr[minIndex] = Integer.MAX_VALUE;  
        }  
          
        /*//判断是否有任务服务时间相同的，再根据请求时间判断   
         * 其实有点多余，请求时间是按升序排序 
         *  
        for(int k=0 ; k<arr2.length-1; k++){   
            if(arr2[k] == arr2[k+1]){ 
                if(requstTime[arr1[k]+1] > requstTime[arr1[k+1]+1]){ 
                    int tmp = arr1[k]; 
                    arr1[k] = arr1[k+1]; 
                    arr1[k+1] = tmp; 
                } 
            } 
        }*/  
          
        /*System.out.println("最大索引数组：");     
        for(int i=0; i<arr1.length; i++){ 
            System.out.print(" " + arr1[i]); 
        } 
        System.out.println(); 
        */  
        return arr1;  
          
    }  
  
    public static void main(String[] args) {  
          
        int[] requestTime = {0,2,4,5};  
        int[] durationTime = {7, 4, 1, 4};   
        int[] a = new int[3];  
          
        ShortJobFirst sjf = new ShortJobFirst();  
        float averageWaitTime = sjf.minAgerageWaitTime(requestTime, durationTime);  
          
//      a = sjf.getMin(requestTime, durationTime);  
          
          
          
        System.out.println();  
        System.out.println("平均等待时间：" + averageWaitTime );  
          
    }  
      
      
}  