import {CheckCircledIcon } from "@radix-ui/react-icons"


interface SuccessFormMessageProps {
    message? : string;
};


export const FormSuccess = ({message}: SuccessFormMessageProps) => {
    if(!message) return null;

    return (
        <div className="bg-emerald-100 p-3 rounded-md flex items-center gap-x-2 text-sm text-emerald-500">
            <CheckCircledIcon className="h-4 w-4"/>
            <span>{message}</span>
        </div>
    )
}