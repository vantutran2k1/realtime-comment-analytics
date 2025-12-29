import torch
from transformers import AutoTokenizer, AutoModelForSequenceClassification

# Cấu hình
MODEL_NAME = "vinai/phobert-base-v2"
MODEL_PATH = "hate_speech_model.pt"
TOKENIZER_PATH = "./tokenizer_config"
class_names = {0: "Bình thường", 1: "Tiêu cực/Xúc phạm nhẹ", 2: "Hate Speech/Chửi tục"}

# Load Tokenizer và Model
device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
tokenizer = AutoTokenizer.from_pretrained(TOKENIZER_PATH)

# Khởi tạo kiến trúc model giống lúc train
model = AutoModelForSequenceClassification.from_pretrained(MODEL_NAME, num_labels=3)
# Load trọng số đã train vào
model.load_state_dict(torch.load(MODEL_PATH))
model.to(device)
model.eval()

def predict(text):
    inputs = tokenizer(text, return_tensors="pt", truncation=True, padding=True, max_length=128)
    inputs = {k: v.to(device) for k, v in inputs.items()}

    with torch.no_grad():
        outputs = model(**inputs)
        probs = torch.nn.functional.softmax(outputs.logits, dim=-1)
        prediction = torch.argmax(probs, dim=-1).item()

    return class_names[prediction], probs[0][prediction].item()

# Test thử
texts = [
    "Bài viết này hay quá, cảm ơn admin.",
    "Thằng này ngu như bò",
    "Chết đi đồ súc sinh"
]

for t in texts:
    label, score = predict(t)
    print(f"Text: {t}\n -> Label: {label} (Độ tin cậy: {score:.2f})\n")