import pandas as pd
import torch
from torch.utils.data import Dataset
from transformers import AutoTokenizer, AutoModelForSequenceClassification, Trainer, TrainingArguments
from sklearn.metrics import accuracy_score, precision_recall_fscore_support
import numpy as np
import os

# --- CẤU HÌNH ---
MODEL_NAME = "vinai/phobert-base-v2"  # Mô hình pre-trained tốt cho tiếng Việt
MAX_LEN = 128         # Độ dài tối đa của câu
BATCH_SIZE = 16       # RTX 3060 12GB có thể chịu được 16 hoặc 32
EPOCHS = 3            # Số vòng lặp train
LEARNING_RATE = 2e-5
NUM_LABELS = 3        # Dựa trên dữ liệu của bạn: 0, 1, 2

# Kiểm tra GPU
device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
print(f"Đang sử dụng thiết bị: {device}")
if device.type == 'cuda':
    print(f"Tên GPU: {torch.cuda.get_device_name(0)}")

# --- 1. CHUẨN BỊ DỮ LIỆU ---
class HateSpeechDataset(Dataset):
    def __init__(self, df, tokenizer, max_len):
        self.df = df
        self.tokenizer = tokenizer
        self.max_len = max_len
        self.texts = df['cmt_col'].values
        self.labels = df['labels'].values

    def __len__(self):
        return len(self.df)

    def __getitem__(self, index):
        text = str(self.texts[index])
        label = int(self.labels[index]) # Chuyển label float về int (0.0 -> 0)

        encoding = self.tokenizer.encode_plus(
            text,
            add_special_tokens=True,
            max_length=self.max_len,
            return_token_type_ids=False,
            padding='max_length',
            truncation=True,
            return_attention_mask=True,
            return_tensors='pt',
        )

        return {
            'input_ids': encoding['input_ids'].flatten(),
            'attention_mask': encoding['attention_mask'].flatten(),
            'labels': torch.tensor(label, dtype=torch.long)
        }

def load_data():
    # Đọc file csv
    print("Đang đọc dữ liệu...")
    train_df = pd.read_csv('train_df.csv')
    val_df = pd.read_csv('val_df.csv')
    test_df = pd.read_csv('test_df.csv')

    # Xử lý dữ liệu cơ bản: Xóa dòng trống (NaN)
    train_df = train_df.dropna(subset=['cmt_col', 'labels'])
    val_df = val_df.dropna(subset=['cmt_col', 'labels'])
    test_df = test_df.dropna(subset=['cmt_col', 'labels'])

    return train_df, val_df, test_df

# --- 2. HÀM TÍNH TOÁN ĐỘ CHÍNH XÁC ---
def compute_metrics(pred):
    labels = pred.label_ids
    preds = pred.predictions.argmax(-1)
    precision, recall, f1, _ = precision_recall_fscore_support(labels, preds, average='macro')
    acc = accuracy_score(labels, preds)
    return {
        'accuracy': acc,
        'f1': f1,
        'precision': precision,
        'recall': recall
    }

# --- MAIN ---
def main():
    # Load Tokenizer
    tokenizer = AutoTokenizer.from_pretrained(MODEL_NAME)

    # Load Data
    train_df, val_df, test_df = load_data()

    print(f"Số lượng mẫu Train: {len(train_df)}")
    print(f"Số lượng mẫu Val: {len(val_df)}")

    # Tạo Dataset
    train_dataset = HateSpeechDataset(train_df, tokenizer, MAX_LEN)
    val_dataset = HateSpeechDataset(val_df, tokenizer, MAX_LEN)
    test_dataset = HateSpeechDataset(test_df, tokenizer, MAX_LEN)

    # Load Model
    model = AutoModelForSequenceClassification.from_pretrained(MODEL_NAME, num_labels=NUM_LABELS)
    model.to(device)

    # Cấu hình Training
    training_args = TrainingArguments(
        output_dir='./results',
        num_train_epochs=EPOCHS,
        per_device_train_batch_size=BATCH_SIZE,
        per_device_eval_batch_size=BATCH_SIZE,
        warmup_steps=100,
        weight_decay=0.01,
        logging_dir='./logs',
        logging_steps=50,
        evaluation_strategy="epoch", # Đánh giá sau mỗi epoch
        save_strategy="no",          # Không save checkpoint rác để tiết kiệm ổ cứng
        learning_rate=LEARNING_RATE,
        fp16=True,                   # Quan trọng: Bật chế độ tính toán nhanh cho RTX 3060
        dataloader_num_workers=2     # Tăng tốc độ load dữ liệu
    )

    # Khởi tạo Trainer
    trainer = Trainer(
        model=model,
        args=training_args,
        train_dataset=train_dataset,
        eval_dataset=val_dataset,
        compute_metrics=compute_metrics
    )

    # Bắt đầu Train
    print("Bắt đầu training...")
    trainer.train()

    # Đánh giá trên tập Test
    print("Đang đánh giá trên tập Test...")
    test_results = trainer.evaluate(test_dataset)
    print("Kết quả trên tập Test:", test_results)

    # --- 3. EXPORT MODEL ---
    print("Đang lưu model...")
    save_path = "hate_speech_model.pt"

    # Cách 1: Lưu toàn bộ state_dict (nhẹ, chuẩn PyTorch, đúng ý bạn)
    torch.save(model.state_dict(), save_path)

    # Lưu thêm tokenizer để lúc dùng (inference) có cái mà dùng
    tokenizer.save_pretrained("./tokenizer_config")

    print(f"Đã lưu model tại: {save_path}")
    print("Đã lưu tokenizer tại thư mục: ./tokenizer_config")

if __name__ == "__main__":
    # Trên Windows, bắt buộc phải đặt code trong block này nếu dùng num_workers > 0
    main()