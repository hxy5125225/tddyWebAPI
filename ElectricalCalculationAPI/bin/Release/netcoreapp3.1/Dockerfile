#See https://aka.ms/containerfastmode to understand how Visual Studio uses this Dockerfile to build your images for faster debugging.

FROM mcr.microsoft.com/dotnet/aspnet:3.1 AS base
FROM mcr.microsoft.com/dotnet/sdk:3.1 AS build
ENV TDDY_PG="Server=47.95.255.108;Port=5432;UserId=postgres;Password=tddy123456;Database=wuxi;Encoding=UTF8;CommandTimeout=300;Pooling=true;MaxPoolSize=75;"
ENV TZ=Asia/Shanghai
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone
EXPOSE 8080
EXPOSE 8080
WORKDIR /app
COPY . /app/
ENTRYPOINT ["dotnet", "ElectricalCalculationAPI.dll"]
