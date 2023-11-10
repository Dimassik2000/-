import numpy as np
import json
import os

file = r'matrix_34.npy' # укажите свой путь
file_output = r'Output' # укажите свой путь
file_output_normalized = os.path.join(file_output, "normalized_matrix.npy") # укажите свой путь

data = np.load(file)
sum_all = np.sum(data)
mean_all = np.mean(data)
sum_main_diag = np.sum(np.diag(data))
mean_main_diag = np.mean(np.diag(data))
sum_side_diag = np.sum(np.diag(np.fliplr(data)))
mean_side_diag = np.mean(np.diag(np.fliplr(data)))
max_val = np.max(data)
min_val = np.min(data)

normalized_data = (data - min_val) / (max_val - min_val)

np.save(file_output_normalized, normalized_data)

results = {
    "sum": int(sum_all),
    "avr": float(mean_all),
    "sumMD": int(sum_main_diag),
    "avrMD": float(mean_main_diag),
    "sumSD": int(sum_side_diag),
    "avrSD": float(mean_side_diag),
    "max": int(max_val),
    "min": int(min_val)
}

with open(os.path.join(file_output, "output.json"), 'w') as f:
    json.dump(results, f)