resnet。  
发现第一次 predict 比后面要慢，比如连续 5 张图片，第一张要 0.1s，后面的只要 0.005s。这是为什么？？

```
model time0 (parameter load time):  0.4313688278198242
model time1 (model to GPU time):  0.10658478736877441
model load time (load parameter + toGPU):  0.5503256320953369
pic preprocesstime 0.00661921501159668
predict time0 (torch no_grad): 1.33514404296875e-05
predict time1 (no_grad + model calculate): 0.1368257999420166
predict time (no_grad + model calculate + get predict_idx): 0.14409089088439941
Predicted class: academic gown
pic preprocesstime 0.0027513504028320312
predict time0 (torch no_grad): 2.0265579223632812e-05
predict time1 (no_grad + model calculate): 0.005121469497680664
predict time (no_grad + model calculate + get predict_idx): 0.006374359130859375
Predicted class: academic gown
pic preprocesstime 0.0023856163024902344
predict time0 (torch no_grad): 1.52587890625e-05
predict time1 (no_grad + model calculate): 0.004835844039916992
predict time (no_grad + model calculate + get predict_idx): 0.006160259246826172
Predicted class: academic gown
pic preprocesstime 0.008936166763305664
predict time0 (torch no_grad): 1.33514404296875e-05
predict time1 (no_grad + model calculate): 0.00475621223449707
predict time (no_grad + model calculate + get predict_idx): 0.0061419010162353516
Predicted class: notebook
```

模型参数加载占据大量时间。提前加载好是好的，随用随加载是不好的。