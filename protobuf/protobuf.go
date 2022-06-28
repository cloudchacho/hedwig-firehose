package protobuf

import (
	"fmt"

	"github.com/Masterminds/semver"
	"github.com/cloudchacho/hedwig-go"
	hedwigProtobuf "github.com/cloudchacho/hedwig-go/protobuf"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type FirehoseEncoderDecoder struct {
	*hedwigProtobuf.EncoderDecoder
}

func (fcd *FirehoseEncoderDecoder) DecodeData(messageType string, version *semver.Version, data interface{}) (interface{}, error) {
	return data, nil
}

// EncodeData encodes the message with appropriate format for transport over the wire
// Type of data must be proto.Message
func (fcd *FirehoseEncoderDecoder) EncodeData(data interface{}, useMessageTransport bool, metaAttrs hedwig.MetaAttributes) ([]byte, error) {
	if useMessageTransport {
		panic("invalid input")
	}
	const urlPrefix = "type.googleapis.com/"
	dst := anypb.Any{}
	// TODO take this as an input to struct
	dst.TypeUrl = urlPrefix + string("standardbase.hedwig.UserCreatedV1")
	dst.Value = data.([]byte)
	container := &hedwigProtobuf.PayloadV1{
		FormatVersion: fmt.Sprintf("%d.%d", metaAttrs.FormatVersion.Major(), metaAttrs.FormatVersion.Minor()),
		Id:            metaAttrs.ID,
		Metadata: &hedwigProtobuf.MetadataV1{
			Publisher: metaAttrs.Publisher,
			Timestamp: timestamppb.New(metaAttrs.Timestamp),
			Headers:   metaAttrs.Headers,
		},
		Schema: metaAttrs.Schema,
		Data:   &dst,
	}
	payload, err := proto.Marshal(container)
	if err != nil {
		// Unable to convert to bytes
		return nil, err
	}
	return payload, nil
}

// VerifyKnownMinorVersion checks that message version is known to us
func (fcd *FirehoseEncoderDecoder) VerifyKnownMinorVersion(messageType string, version *semver.Version) error {
	// no minor verification
	return nil
}

// EncodeMessageType encodes the message type with appropriate format for transport over the wire
func (fcd *FirehoseEncoderDecoder) EncodeMessageType(messageType string, version *semver.Version) string {
	return fmt.Sprintf("%s/%d.%d", messageType, version.Major(), version.Minor())
}

func (fcd *FirehoseEncoderDecoder) IsBinary() bool {
	return true
}
