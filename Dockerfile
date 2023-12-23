FROM python:3.11

COPY . .

RUN pip install -r requirements.txt

RUN pip install --upgrade pip setuptools wheel

RUN python /preprocess/imports.py
# RUN python extract_all_offers_exception.py

CMD [ "python","app.py" ]
