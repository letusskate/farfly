import torch
from torchvision import models
from torchvision import transforms
from PIL import Image
import matplotlib.pyplot as plt
import time

now_time = time.time()
# 加载预训练的 ResNet-50 模型
model = models.resnet50(pretrained=True)
print("model time0 (parameter load time): ",time.time()-now_time)
device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
if not torch.cuda.is_available():
    print("CUDA is not available. Using CPU")
todevicebegin_time = time.time()
model = model.to(device)
modelloadtime = time.time()-now_time
print("model time1 (model to GPU time): ",time.time()-todevicebegin_time)
print("model load time (load parameter + toGPU): ",modelloadtime)
model.eval()  # 设置为评估模式
for i in range(4):
    now_time = time.time()
    # 准备输入图像
    image_path = f"test_image{i}.jpg"  # 替换为你的测试图像路径
    image = Image.open(image_path)

    # 定义图像预处理
    preprocess = transforms.Compose([
        transforms.Resize(256),
        transforms.CenterCrop(224),
        transforms.ToTensor(),
        transforms.Normalize(mean=[0.485, 0.456, 0.406], std=[0.229, 0.224, 0.225]),
    ])

    # 预处理图像
    input_tensor = preprocess(image)
    input_batch = input_tensor.unsqueeze(0)  # 添加批量维度
    input_batch = input_batch.to(device)
    picprocesstime = time.time()-now_time
    print("pic preprocesstime",picprocesstime)

    now_time = time.time()
    # 运行模型
    with torch.no_grad():
        print("predict time0 (torch no_grad):", time.time()-now_time)
        output = model(input_batch)
    print("predict time1 (no_grad + model calculate):", time.time()-now_time)
    # 获取预测结果
    _, predicted_idx = torch.max(output, 1)
    predicted_idx = predicted_idx.item()
    print("predict time (no_grad + model calculate + get predict_idx):", time.time()-now_time)

    # 加载类别名称
    with open("imagenet_classes.txt", "r") as f:
        classes = [line.strip() for line in f.readlines()]

    # 打印预测结果
    predicted_class = classes[predicted_idx]
    print(f"Predicted class: {predicted_class}")

    # 显示图像
    plt.imshow(image)
    plt.title(f"Predicted: {predicted_class}")
    plt.show()