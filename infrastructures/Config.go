package infrastructures

import (
	"context"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/go-sql-driver/mysql"
	"github.com/spf13/viper"
)

func prepareConfig() {
	viper.AutomaticEnv()

	viper.SetConfigName("config")
	viper.SetConfigType("json")
	viper.AddConfigPath(".")
	err := viper.ReadInConfig()
	if err != nil {
		panic(fmt.Errorf("fatal error config file: %w", err))
	}
}

func GetMySQLConfig() mysql.Config {
	prepareConfig()

	mySQLConfig := mysql.Config{
		User:      viper.GetString("db_user"),
		Passwd:    viper.GetString("db_password"),
		Net:       "tcp",
		Addr:      viper.GetString("db_address"),
		DBName:    viper.GetString("db_name"),
		ParseTime: true,
	}

	return mySQLConfig
}

func GetS3Credentials() credentials.StaticCredentialsProvider {
	return credentials.NewStaticCredentialsProvider(
		viper.GetString("s3_key"),
		viper.GetString("s3_secret"),
		viper.GetString("s3_session"),
	)
}

func GetS3ConnectionConfig() aws.Config {
	prepareConfig()

	cfg, err := config.LoadDefaultConfig(context.TODO(),
		config.WithCredentialsProvider(GetS3Credentials()),
	)
	if err != nil {
		panic("Couldn't init S3 config")
	}
	cfg.BaseEndpoint = aws.String(viper.GetString("s3_endpoint"))

	return cfg
}

func GetConfigBucketName() string {
	return viper.GetString("s3_bucket")
}

func GetTempFileName() string {
	return viper.GetString("temp_file_name")
}
