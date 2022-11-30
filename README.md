# Тестовое задание
Программа, выполняющая две функции:

1. Создание 50 zip-архивов, в каждом 100 xml файлов со случайными данными следующей структуры:

```xml
<root>
    <var name='id' value='<случайное уникальное строковое значение>'/>
    <var name='level' value='<случайное число от 1 до 100>'/>
    <objects>
        <object name='<случайное строковое значение>'/>
        <object name='<случайное строковое значение>'/>
        …
    </objects>
</root>
```

В тэге objects случайное число (от 1 до 10) вложенных тэгов object.

2. Обработка директории с созданными архивами и на основе данных в xml-файлах формирование двух csv-файлов
   1. `id, level` — по строке на каждый xml-файл.
   2. `id, object_name` — по строке для каждого тэга object (от 1 до 10 на каждый xml-файл)