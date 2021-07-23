#setup python image
FROM python:3.7
ENV PYTHONUNBUFFERED=1

#ENV HTTP_PROXY must be set for UCLH environment - may not be required for other Trusts
# ENV HTTP_PROXY=xxx
# ENV http_proxy=xxx
# ENV HTTPS_PROXY=xxx
# ENV https_proxy=xxx

RUN mkdir /code
WORKDIR /code
COPY . /code/

RUN pip install -r requirements.txt

RUN python -m spacy download en_core_web_sm
RUN pip install https://s3-us-west-2.amazonaws.com/ai2-s2-scispacy/releases/v0.2.4/en_core_sci_md-0.2.4.tar.gz


