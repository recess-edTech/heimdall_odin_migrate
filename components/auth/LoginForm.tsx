import {CardWrapper} from "@/components/auth/CardWrapper";

export const LoginForm = () => {
    return (
        <CardWrapper
            headerLabel="Welcome Back"
            backButtonLabel="Don't Have an Account?"
            backButtonHref="/auth/register"
            showSocial={true}>
            <div>
                Login from
            </div>
        </CardWrapper>
    )
}
