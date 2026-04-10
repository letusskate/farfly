import transformers
import torch
from modelscope import snapshot_download

print(f"Transformers version: {transformers.__version__}")

# cnt = 0
# while cnt<5000:
#     try:
#         model_id = snapshot_download("LLM-Research/Meta-Llama-3-8B-Instruct")
#         break
#     except:
#         cnt = cnt+1
#         print('!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!')
#         print('download stop, retry cnt=',cnt)
#         print('!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!') 

model_id = snapshot_download("LLM-Research/Meta-Llama-3-8B-Instruct")
print(model_id)


pipeline = transformers.pipeline(
    "text-generation",
    model=model_id,
    model_kwargs={"torch_dtype": torch.bfloat16},
    device="cuda",
)

messages = [
    {"role": "system", "content": "You are a pirate chatbot who always responds in pirate speak!"},
    {"role": "user", "content": "Who are you?"},
]

prompt = pipeline.tokenizer.apply_chat_template(
		messages, 
		tokenize=False, 
		add_generation_prompt=True
)

terminators = [
    pipeline.tokenizer.eos_token_id,
    pipeline.tokenizer.convert_tokens_to_ids("<|eot_id|>")
]

outputs = pipeline(
    prompt,
    max_new_tokens=256,
    eos_token_id=terminators,
    do_sample=True,
    temperature=0.6,
    top_p=0.9,
)
print(outputs[0]["generated_text"][len(prompt):])

