namespace Messaging.Abstractions.Rpc;

public class RpcResponseEnvelope
{
    public bool Success { get; set; }

    public string? ErrorCode { get; set; }

    public string? ErrorMessage { get; set; }

    public byte[]? Payload { get; set; }
}
