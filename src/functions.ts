import { SayHelloProps } from "./types"

export function sayHello({firstName, lastName, age}:SayHelloProps) {
    console.log(`values, ${firstName}`)
    console.log(age)
    if (lastName) {
        console.log(`values ${lastName}`)
    }
}
