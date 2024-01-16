"use client"
import {useForm} from "react-hook-form";
import {zodResolver} from "@hookform/resolvers/zod";
import {FormControl, Form, FormItem, FormLabel, FormField, FormMessage} from "@/components/ui/form";
import {CardWrapper} from "@/components/auth/CardWrapper";
import * as z from "zod";
import {LoginSchema} from "@/schemas";
import {Input} from "@/components/ui/input";


export const LoginForm = () => {
    const form = useForm<z.infer<typeof LoginSchema>>({
        resolver: zodResolver(LoginSchema),
        defaultValues: {
            email: "",
            password: "",
        }
    })
    return (
        <CardWrapper
            headerLabel="Welcome Back"
            backButtonLabel="Don't Have an Account?"
            backButtonHref="/auth/register"
            showSocial={true}>
            <Form {...form}>
                <form
                    onSubmit={form.handleSubmit(() => {
                    })}
                    className="space-y-6"
                />
                <div className="space-y-4">
                    <FormField
                        control={form.control}
                        name="email"
                        render={({field}) => (
                            <FormItem>
                                <FormLabel>Email</FormLabel>
                                <FormControl>
                                    <Input {...field} 
                                            placeholder="example@gmail.com"
                                           type="email"
                                    />
                                </FormControl>
                                <FormMessage/>
                            </FormItem>
                        )}/>
                </div>
            </Form>
        </CardWrapper>
    )
}
