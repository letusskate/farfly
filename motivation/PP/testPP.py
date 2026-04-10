#----------------------------------------------------#
#   将单张图片预测、摄像头检测和FPS测试功能
#   整合到了一个py文件中，通过指定mode进行模式的修改。
#----------------------------------------------------#
import time

import cv2
import numpy as np
from PIL import Image
import torch
from deeplabPP import DeeplabV3

if __name__ == "__main__":

    deeplab = DeeplabV3()

    if torch.distributed.get_rank() == 0:
        mode = "dir_predict"
        count           = False
        name_classes    = ["background","aeroplane", "bicycle", "bird", "boat", "bottle", "bus", "car", "cat", "chair", "cow", "diningtable", "dog", "horse", "motorbike", "person", "pottedplant", "sheep", "sofa", "train", "tvmonitor"]
        dir_origin_path = "img/"
        dir_save_path   = "img_out/"

        simplify        = True
        onnx_save_path  = "model_data/models.onnx"

        if mode == "predict":
            while True:
                img = input('Input image filename:')
                try:
                    image = Image.open(img)
                except:
                    print('Open Error! Try again!')
                    continue
                else:
                    r_image = deeplab.detect_image(image, count=count, name_classes=name_classes)
                    r_image.show()
                    r_image.save("img.jpg")

            
        elif mode == "dir_predict":
            import os
            from tqdm import tqdm

            img_names = os.listdir(dir_origin_path)
            for img_name in tqdm(img_names):
                if img_name.lower().endswith(('.bmp', '.dib', '.png', '.jpg', '.jpeg', '.pbm', '.pgm', '.ppm', '.tif', '.tiff')):
                    image_path  = os.path.join(dir_origin_path, img_name)
                    image       = Image.open(image_path)
                    r_image     = deeplab.detect_image(image)
                    if not os.path.exists(dir_save_path):
                        os.makedirs(dir_save_path)
                    r_image.save(os.path.join(dir_save_path, img_name))
        
        else:
            raise AssertionError("Please specify the correct mode: 'predict', 'video', 'fps' or 'dir_predict'.")
