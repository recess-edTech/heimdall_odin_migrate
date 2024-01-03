"use client";

import React from "react";

interface LoginButtonProps {
    children: React.ReactNode;
    mode?: "modal" | "redirect";
    asChild?: boolean;
}


export const LoginButton = ({children, mode = "redirect", asChild}: LoginButtonProps) => {
    function onClick() {
        console.log("login click handler")
    }

    return (
        <span className="pointer" onClick={onClick}>
            {children}
        </span>
    )
}
