package main

import (
	"encoding/json"
	"fmt"
	"os"
	"os/signal"
	"path"
	"syscall"

	"github.com/dgraph-io/badger"
	account "github.com/koinos/koinos-contract-meta-store/internal/metastore"
	log "github.com/koinos/koinos-log-golang"
	koinosmq "github.com/koinos/koinos-mq-golang"
	"github.com/koinos/koinos-proto-golang/koinos/broadcast"
	"github.com/koinos/koinos-proto-golang/koinos/contracts/token"
	"google.golang.org/protobuf/proto"

	util "github.com/koinos/koinos-util-golang"
	flag "github.com/spf13/pflag"
)

const (
	basedirOption    = "basedir"
	amqpOption       = "amqp"
	instanceIDOption = "instance-id"
	logLevelOption   = "log-level"
	resetOption      = "reset"
)

const (
	basedirDefault    = ".koinos"
	amqpDefault       = "amqp://guest:guest@localhost:5672/"
	instanceIDDefault = ""
	logLevelDefault   = "info"
	resetDefault      = false
)

const (
	accountHistoryRPC = "account_history"
	transactionAccept = "koinos.transaction.accept"
	appName           = "account_history"
	logDir            = "logs"
)

func main() {
	var baseDir = flag.StringP(basedirOption, "d", basedirDefault, "the base directory")
	var amqp = flag.StringP(amqpOption, "a", "", "AMQP server URL")
	var reset = flag.BoolP("reset", "r", false, "reset the database")
	instanceID := flag.StringP(instanceIDOption, "i", instanceIDDefault, "The instance ID to identify this service")
	logLevel := flag.StringP(logLevelOption, "v", logLevelDefault, "The log filtering level (debug, info, warn, error)")

	flag.Parse()

	*baseDir = util.InitBaseDir(*baseDir)

	yamlConfig := util.InitYamlConfig(*baseDir)

	*amqp = util.GetStringOption(amqpOption, amqpDefault, *amqp, yamlConfig.ContractMetaStore, yamlConfig.Global)
	*logLevel = util.GetStringOption(logLevelOption, logLevelDefault, *logLevel, yamlConfig.ContractMetaStore, yamlConfig.Global)
	*instanceID = util.GetStringOption(instanceIDOption, util.GenerateBase58ID(5), *instanceID, yamlConfig.ContractMetaStore, yamlConfig.Global)
	*reset = util.GetBoolOption(resetOption, resetDefault, *reset, yamlConfig.ContractMetaStore, yamlConfig.Global)

	appID := fmt.Sprintf("%s.%s", appName, *instanceID)

	// Initialize logger
	logFilename := path.Join(util.GetAppDir(*baseDir, appName), logDir, "account_history.log")
	err := log.InitLogger(*logLevel, false, logFilename, appID)
	if err != nil {
		panic(fmt.Sprintf("Invalid log-level: %s. Please choose one of: debug, info, warn, error", *logLevel))
	}

	// Costruct the db directory and ensure it exists
	dbDir := path.Join(util.GetAppDir((*baseDir), appName), "db")
	util.EnsureDir(dbDir)
	log.Infof("Opening database at %s", dbDir)

	var opts = badger.DefaultOptions(dbDir)
	opts.Logger = account.KoinosBadgerLogger{}
	var backend = account.NewBadgerBackend(opts)

	// Reset backend if requested
	if *reset {
		log.Info("Resetting database")
		err := backend.Reset()
		if err != nil {
			panic(fmt.Sprintf("Error resetting database: %s\n", err.Error()))
		}
	}

	defer backend.Close()

	requestHandler := koinosmq.NewRequestHandler(*amqp)
	//accountHistory := metastore.NewAccountHistory(backend)

	requestHandler.SetBroadcastHandler("koinos.event.19JntSm8pSNETT9aHTwAUHC5RMoaSmgZPJ.koin.mint", func(topic string, data []byte) {
		log.Debug("Received event!")

		parcel := &broadcast.EventParcel{}

		if err := proto.Unmarshal(data, parcel); err != nil {
			log.Warnf("Unable to parse event broadcast: %v", data)
			return
		}

		jsonParcel, _ := json.Marshal(parcel)
		log.Debugf("Event: %s", jsonParcel)

		switch parcel.Event.Name {
		case "koin.mint":
			mint := &token.MintEvent{}
			if err := proto.Unmarshal(parcel.Event.Data, mint); err != nil {
				log.Warnf("Unable to parse event broadcast: %v", data)
				return
			}
			mintJSON, _ := json.Marshal(mint)
			log.Debugf("Mint Data: %s", mintJSON)
		case "koin.transfer":
			transfer := &token.TransferEvent{}
			if err := proto.Unmarshal(parcel.Event.Data, transfer); err != nil {
				log.Warnf("Unable to parse event broadcast: %v", data)
				return
			}
			transferJSON, _ := json.Marshal(transfer)
			log.Debugf("Transfer Data: %s", transferJSON)
		}

		//log.Debugf("Received event - %s", parcel.Event.Name)

	})

	requestHandler.Start()

	// Wait for a SIGINT or SIGTERM signal
	ch := make(chan os.Signal, 1)
	signal.Notify(ch, syscall.SIGINT, syscall.SIGTERM)
	<-ch
	log.Info("Shutting down node...")
}
