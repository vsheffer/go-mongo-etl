// Package to support building a Mongo OpLog tailer.
//
// Package allows a developer to implement an interface that handles
// Mongo OpLog events for insert, update and deletions.  The package
// will start tailing the log and upon detecting one of the events above
// will dispatch in a separate Go routine to the relevant method provided
// by the developer.
package gooplog

import (
	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
	"log"
	"time"
)

// The interface that must be implemented and registered with OpLogTailer
// class so that the events can be dispatched to it.
type OpLogger interface {
	// Method called when the OpLogTailer receives a deletion operation.
	OnDelete(deleted bson.M)

	// Method called when the OpLogTailer receives an update operation.
	OnUpdate(updated bson.M)

	// Method called when the OpLogTailer receives an insert operation.
	OnInsert(inserted bson.M)
}

type OpLogTailer struct {
	collectionToTail *string
	session          *mgo.Session
	opLogger         OpLogger
}

// Internal Mongo collection maintained by the OpLog tailer to keep track of information, like
// the timestamp from which it should start reading on restart.
type opLogTailerInfo struct {
	FilterRegex          string              "filterRegex"
	StartReadingFromTime bson.MongoTimestamp `bson:"startReadingFromTime"`
	Label                string              `bson:"label"`
}

type ObjectId struct {
	Id string `bson:"_id"`
}

type opLogEntry struct {
	Ts bson.MongoTimestamp "ts"
	V  string              "v"
	Op string              "op"
	Ns string              "ns"
	O  bson.M              "o"
	O2 ObjectId            `bson:"o2,inline"`
}

var tailerInfoCollection *mgo.Collection
var session *mgo.Session
var tailerInfo *opLogTailerInfo

// Create a new OpLogTailer.
func NewOpLogTailer(url *string, filterRegex *string, label *string, opLogger OpLogger) *OpLogTailer {
	var err error

	// Open the Mongo DB session to be shared for all connections.
	seconds, _ := time.ParseDuration("0s")
	session, err = mgo.DialWithTimeout(*url, seconds)
	if err != nil {
		log.Fatalf("Can't open connection to %s: %s", url, err)
	}

	// Create/open the gooplog Mongo database and create/open the opLogTailerInfo collection.
	tailerInfoCollection = session.DB("gooplog").C("opLogTailerInfo")

	tailerInfo = &opLogTailerInfo{
		FilterRegex:          *filterRegex,
		StartReadingFromTime: bson.MongoTimestamp(time.Now().Unix() << 32),
		Label:                *label}

	// Read the opLogTailerInfo for the collection to be tailed.
	query := tailerInfoCollection.Find(buildOpLogTailerInfoSelector())
	var count int
	count, err = query.Count()
	if err != nil {
		log.Fatalf("Error getting count for opLogTailerInfo: %+v", err)
	}

	if count > 1 {
		log.Fatalf("The gooplog collection opLogTailerInfo has more than one document for tailed collection [%s] and label [%s].\n  There should only be one document per tailed collection and label.  Pleaes correct and restart.", tailerInfo.FilterRegex, tailerInfo.Label)
	}

	if count == 0 {

		// There isn't a document yet for collection and label, so create on.
		log.Printf("Creating %+v", tailerInfo)
		tailerInfoCollection.Insert(tailerInfo)
	} else {

		// We've eliminated the count > 1 and count == 0.
		// This must mean there is exactly 1, so read it in.

		query.One(&tailerInfo)
		log.Printf("Read %+v", tailerInfo)
	}

	return &OpLogTailer{
		session:  nil,
		opLogger: opLogger}
}

func buildOpLogTailerInfoSelector() bson.M {
	var andClause [2]bson.M

	andClause[0] = bson.M{"filterRegex": tailerInfo.FilterRegex}
	andClause[1] = bson.M{"label": tailerInfo.Label}

	opLogTailerInfoSelector := bson.M{"$and": andClause}
	log.Printf("opLogTailerInfoSelector  = %+v", opLogTailerInfoSelector)
	return opLogTailerInfoSelector
}

func buildOpLogSelector() bson.M {
	var andClause [2]bson.M

	andClause[0] = bson.M{"ts": bson.M{"$gt": tailerInfo.StartReadingFromTime}}
	andClause[1] = bson.M{"ns": bson.M{"$regex": bson.RegEx{tailerInfo.FilterRegex, ""}}}

	opLogSelector := bson.M{"$and": andClause}
	log.Printf("opLogSelector = %+v", opLogSelector)
	return opLogSelector
}

func (olt *OpLogTailer) Start() error {
	collection := session.DB("local").C("oplog.rs")
	log.Printf("coll = %+v", collection)

	iter := collection.Find(buildOpLogSelector()).LogReplay().Sort("$natural").Tail(-1)

	var result opLogEntry
	for {
		for iter.Next(&result) {
			log.Printf("result = %+v", result)
			go func() {
				tailerInfo.StartReadingFromTime = result.Ts
				tailerInfoCollection.Update(bson.M{"filterRegex": tailerInfo.FilterRegex, "label": tailerInfo.Label}, tailerInfo)
			}()

			go func(result opLogEntry) {
				switch result.Op {
				case "u":
					olt.opLogger.OnUpdate(result.O)
					return
				case "i":
					olt.opLogger.OnInsert(result.O)
					return
				case "d":
					olt.opLogger.OnDelete(result.O)
					return
				}
			}(result)
		}

		if iter.Err() != nil {
			log.Printf("Got error: %+v", iter.Err())
			return iter.Close()
		}

		if iter.Timeout() {
			continue
		}

		// If we are here, it means something other than a timeout occurred, so let's
		// try and restart the tailing cursor.
		query := collection.Find(buildOpLogSelector())
		iter = query.Sort("$natural").Tail(5 * time.Second)
	}
	return iter.Close()
}
