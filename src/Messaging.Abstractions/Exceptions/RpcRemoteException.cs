namespace Messaging.Abstractions.Exceptions;

public class RpcRemoteException : Exception
{
    public string ErrorCode { get; }
    public RpcRemoteException(string errorCode, string message) : base(message)
    {
        ErrorCode = errorCode;
    }
}
