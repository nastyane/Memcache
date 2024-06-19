package ru.nastya;

import java.io.Serializable;
import java.util.Properties;


public interface Partitions {
    Properties getProperties();

    // Метод для добавления значения
    Serializable putValue(Serializable key, Serializable value);

    // Метод для извлечения значения
    Serializable getValue(Serializable key);



}

