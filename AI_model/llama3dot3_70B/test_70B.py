import transformers
import torch
from modelscope import snapshot_download

# model_id = snapshot_download("LLM-Research/Llama-3.3-70B-Instruct")

cnt = 0
while cnt<5000:
    try:
        model_id = snapshot_download("LLM-Research/Llama-3.3-70B-Instruct")
        break
    except:
        cnt = cnt+1
        print('!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!')
        print('download stop, retry cnt=',cnt)
        print('!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!') 


pipeline = transformers.pipeline(
    "text-generation",
    model=model_id,
    model_kwargs={"torch_dtype": torch.bfloat16},
    device_map="auto",
)

messages = [
    {"role": "system", "content": "You are a pirate chatbot who always responds in pirate speak!"},
    {"role": "user", "content": "Who are you?"},
]

outputs = pipeline(
    messages,
    max_new_tokens=256,
)
print(outputs[0]["generated_text"][-1])