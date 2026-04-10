## 不预处理=GPU无气泡？
如果是，那么不预处理时候，开多个bash同时跑，总帧率应该降低。
但显然不是这样，因为1task只能吃75%gpu算力。
### 2 task
```
predict time: 0.009445428848266602
Predicted class: academic gown
predict time: 0.009546279907226562
Predicted class: academic gown
predict time: 0.009337902069091797
Predicted class: academic gown
predict time: 0.009591102600097656
Predicted class: academic gown
predict time: 0.009459257125854492
Predicted class: academic gown
predict time: 0.009434700012207031
Predicted class: academic gown
predict time: 0.009451627731323242
Predicted class: academic gown
total frames:  2674
```
### 1 task
```
predict time: 0.0053501129150390625
Predicted class: academic gown
predict time: 0.005357265472412109
Predicted class: academic gown
predict time: 0.005453824996948242
Predicted class: academic gown
predict time: 0.005338430404663086
Predicted class: academic gown
predict time: 0.005383014678955078
Predicted class: academic gown
predict time: 0.005368709564208984
Predicted class: academic gown
predict time: 0.00536346435546875
Predicted class: academic gown
predict time: 0.00538945198059082
Predicted class: academic gown
predict time: 0.005411386489868164
Predicted class: academic gown
predict time: 0.005431413650512695
Predicted class: academic gown
predict time: 0.005362033843994141
Predicted class: academic gown
predict time: 0.0054090023040771484
Predicted class: academic gown
predict time: 0.005347728729248047
Predicted class: academic gown
predict time: 0.0053806304931640625
Predicted class: academic gown
predict time: 0.0053958892822265625
Predicted class: academic gown
total frames:  4511
```

## 是否不预处理就相当于batch size提高了呢？（批处理的重要性）
显然不是（single_task_no_preprocess），单个图片的预测时间仍然是0.005s。
predict time: 0.0054094791412353516
Predicted class: academic gown
predict time: 0.005469322204589844
Predicted class: academic gown
predict time: 0.005471944808959961
Predicted class: academic gown
predict time: 0.005296230316162109
Predicted class: academic gown
predict time: 0.005556821823120117
Predicted class: academic gown
predict time: 0.005453586578369141
Predicted class: academic gown
predict time: 0.005327463150024414
Predicted class: academic gown
predict time: 0.005483388900756836
Predicted class: academic gown
predict time: 0.0054471492767333984
Predicted class: academic gown
predict time: 0.005357265472412109
Predicted class: academic gown
predict time: 0.005501985549926758
Predicted class: academic gown
predict time: 0.005496025085449219
Predicted class: academic gown
predict time: 0.005368471145629883
Predicted class: academic gown
predict time: 0.0054585933685302734
Predicted class: academic gown
predict time: 0.005427837371826172
Predicted class: academic gown
predict time: 0.00548553466796875
Predicted class: academic gown
predict time: 0.005475759506225586
Predicted class: academic gown
predict time: 0.005421876907348633
Predicted class: academic gown
predict time: 0.005513429641723633
Predicted class: academic gown
predict time: 0.0065119266510009766
Predicted class: academic gown
predict time: 0.00560307502746582
Predicted class: academic gown
total frames:  4466



## batch_size实验

batch size 1: 3685

batch size 2: 6732

batch size 4: 10672

batch size 8: 13608

batch size 16: 16576

batch size 32: 17420

batch size 64: 17792

batch size 128: 18560

batch size 256: 19200

batch size 512: 19456

batch size 1024: 19456

### 多终端
1终端batchsize32，gpu资源利用率90%，气泡肯定还是有的
2终端任务并行batchsize32，还是18000左右，gpu资源利用率100%


### 部分实验数据
#### 1
```
Image 1 predicted class: academic gown
pic preprocesstime 0.002321958541870117
predict time: 0.005606174468994141
Image 1 predicted class: academic gown
pic preprocesstime 0.002311229705810547
predict time: 0.0056073665618896484
Image 1 predicted class: academic gown
pic preprocesstime 0.002100706100463867
predict time: 0.0055866241455078125
Image 1 predicted class: academic gown
pic preprocesstime 0.0022919178009033203
predict time: 0.0055768489837646484
Image 1 predicted class: academic gown
pic preprocesstime 0.0021190643310546875
predict time: 0.005591154098510742
Image 1 predicted class: academic gown
total frames:  3685
```


#### 32
```
pic preprocesstime 0.0076084136962890625
predict time: 0.04630637168884277
pic preprocesstime 0.006548404693603516
predict time: 0.046305179595947266
pic preprocesstime 0.006017208099365234
predict time: 0.04635739326477051
pic preprocesstime 0.0056629180908203125
predict time: 0.04637908935546875
pic preprocesstime 0.007632017135620117
predict time: 0.046352386474609375
pic preprocesstime 0.006537675857543945
predict time: 0.04636263847351074
pic preprocesstime 0.005997896194458008
predict time: 0.046384572982788086
pic preprocesstime 0.005631923675537109
predict time: 0.046320199966430664
pic preprocesstime 0.007631063461303711
predict time: 0.04637742042541504
pic preprocesstime 0.006585359573364258
predict time: 0.046387672424316406
pic preprocesstime 0.0058841705322265625
predict time: 0.04634571075439453
pic preprocesstime 0.00554203987121582
predict time: 0.04634737968444824
total frames:  17920
```

## summary
为什么batch size提高，帧率也提高？
首先，当输入的任务过小，GPU的算力节点并没有被充分使用，因此将多个任务组合在一起可以更好地利用GPU并行资源。
其次，batchsize增大可以减少GPU气泡。