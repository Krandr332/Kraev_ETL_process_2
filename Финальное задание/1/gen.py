import csv
import random
from datetime import datetime, timedelta


def generate_transactions(filename, target_size_mb=30):
    """Генерирует CSV-файл с транзакциями указанного размера"""

    # Варианты значений для реалистичных данных
    regions = ['DE-HE', 'DE-BE', 'DE-BY', 'DE-NW', 'DE-SH']
    campaign_types = ['credit_card_offer', 'loan_offer', 'insurance_offer', 'deposit_offer']
    call_statuses = ['answered', 'missed', 'busy', 'rejected']
    client_responses = ['interested', 'not_interested', 'call_back', 'already_client', '']

    target_bytes = target_size_mb * 1024 * 1024
    current_size = 0
    row_count = 0

    with open(filename, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        # Заголовки
        writer.writerow(['call_id', 'call_time', 'client_id', 'region_code',
                         'campaign_type', 'call_status', 'client_response',
                         'duration_sec', 'follow_up_required'])

        while current_size < target_bytes:
            row_count += 1
            # Генерируем запись
            call_date = datetime.now() - timedelta(days=random.randint(0, 365))
            call_id = f"call_{call_date.strftime('%Y%m%d')}_{row_count:04d}"

            row = [
                call_id,
                call_date.strftime('%Y-%m-%d %H:%M:%S'),
                f"client_{random.randint(1000, 99999)}",
                random.choice(regions),
                random.choice(campaign_types),
                random.choice(call_statuses),
                random.choice(client_responses),
                random.randint(10, 1200),
                random.choice(['true', 'false'])
            ]
            writer.writerow(row)

            # Примерный подсчет размера (приблизительно)
            current_size = f.tell()

            if row_count % 1000 == 0:
                print(f"Сгенерировано {row_count} строк (~{current_size / 1024 / 1024:.1f} MB)")

    print(f"✅ Готово! Сгенерировано {row_count} строк, размер файла: {current_size / 1024 / 1024:.1f} MB")
    return row_count


# Запускаем генерацию
generate_transactions('../transactions_v2.csv', target_size_mb=30)