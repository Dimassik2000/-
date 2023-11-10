import numpy as np
import os

# Путь к файлу с матрицей
file = r'matrix_34_2.npy' # укажите свой путь
file_output = r'Output2' # укажите свой путь

# Загрузка матрицы из файла
matrix = np.load(file)

# вариант
variant = 34

# Создание массивов x, y, z
x, y, z = np.where(matrix > (500 + variant)), np.where(matrix > (500 + variant))[0], np.where(matrix > (500 + variant))[1]

# Сохранение полученных массивов в файл формата npz
np.savez(file_output + '\\result.npz', x=x, y=y, z=z)
np.savez_compressed(file_output + '\\result_compressed.npz', x=x, y=y, z=z)

# Сравнение размеров полученных файлов
print("Размер файла result.npz:", os.path.getsize(file_output + '\\result.npz'), "байт")
print("Размер файла result_compressed.npz:", os.path.getsize(file_output + '\\result_compressed.npz'), "байт")