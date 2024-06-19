package ru.nastya;

public class Client {
    public static void main(String[] args) {
        int partitionPrevious = 2;
        int partitionNow = 3;
        new Affinity(partitionPrevious, partitionNow).getPartitions(partitionPrevious);
        System.out.println("----------------------------------");
        new Affinity(partitionPrevious, partitionNow).PartitionsCountNow(partitionNow);
    }
}