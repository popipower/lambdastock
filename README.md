# lambdastock
Lambda Stocks

docker build -t lambda-stock .   
docker images
docker tag <image_id> <repo-id>.dkr.ecr.us-east-2.amazonaws.com/lambda-stock:<image_id>
docker push <repo-id>.dkr.ecr.us-east-2.amazonaws.com/lambda-stock:<image_id>

aws ecr get-login-password --region us-east-2 | docker login --username AWS --password-stdin <repo-id>.dkr.ecr.us-east-2.amazonaws.com

